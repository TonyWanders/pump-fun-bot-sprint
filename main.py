"""Momentum bot for the make.fun Pump.fun Bot Sprint.

Reads the agent-fun/replay/v2 stream on stdin and trades paper SOL on a
replay of pump.fun bonding curve events. Strategy, in short:

- Watch every new coin from its CreateEvent and track its price
  (virtual SOL reserves divided by virtual token reserves), trade count,
  and the buy share of its recent SOL volume.
- Buy a coin once it has risen 30 percent above its first seen price on
  buy dominated flow, with at least 5 trades behind it and enough real
  reserves for a useful order size. Stake is 0.2 SOL per coin, at most 5
  open positions, and each coin is entered at most once per run.
- Exit with discipline: sell half at plus 50 percent, sell the rest at
  plus 120 percent, trail 20 percent below the peak after the first take
  profit, stop out at minus 20 percent, and dump a flat coin after 60
  trades if it is still under entry plus 10 percent.

The dev window this was tuned against shows why: about 39 percent of
coins spike at least 1.5x from creation, but under 4 percent end the
window above 1.5x, so most coins pump and then die. The bot rides the
early move and gets out fast instead of holding to the end.

Standard library only. Deterministic: no clock, no randomness, no
environment reads, no iteration over sets. Two runs on the same data
produce the same action log.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "vendor"))

import makefun_bot  # noqa: E402

LAMPORTS_PER_SOL = makefun_bot.LAMPORTS_PER_SOL

STAKE_LAMPORTS = 200_000_000            # 0.2 SOL per entry
MIN_BUY_LAMPORTS = 50_000_000           # skip entries smaller than 0.05 SOL
ENTRY_RISE_BPS = 3000                   # buy at 30 percent above first price
ENTRY_MIN_TRADES = 5
ENTRY_MAX_TRADES = 120                  # only enter while the coin is still young
ENTRY_MIN_REAL_SOL = 5 * LAMPORTS_PER_SOL  # need 5 SOL real reserves for a useful cap
ENTRY_BUY_SHARE_BPS = 6000              # buys must be 60 percent of recent SOL volume
RECENT_TRADES = 10
MAX_SLIPPAGE_BPS = 2000
STOP_BPS = 2000                         # sell all at 20 percent below entry
TP1_BPS = 5000                          # sell half at 50 percent above entry
TP2_BPS = 12000                         # sell the rest at 120 percent above entry
TRAIL_BPS = 2000                        # after TP1, sell the rest 20 percent below peak
TIME_EXIT_TRADES = 60
TIME_EXIT_BPS = 1000                    # sell if under entry plus 10 percent after 60 trades


def price_of(curve):
    """SOL per token from virtual reserves, as a float ratio."""
    token = curve.virtual_token_reserves
    if token <= 0:
        return 0.0
    return curve.virtual_quote_reserves / token


class Coin:
    """Per coin tracking, in first seen order."""

    __slots__ = (
        "first_price",
        "trades",
        "recent",
        "buy_vol",
        "sell_vol",
        "entered",
        "entry_price",
        "peak",
        "trades_at_entry",
        "tp1_done",
        "done",
    )

    def __init__(self, first_price):
        self.first_price = first_price
        self.trades = 0
        self.recent = []          # list of (is_buy, sol_lamports), oldest first
        self.buy_vol = 0
        self.sell_vol = 0
        self.entered = False      # a buy for this coin has filled
        self.entry_price = 0.0
        self.peak = 0.0
        self.trades_at_entry = 0
        self.tp1_done = False
        self.done = False         # traded already, never re-enter this coin


coins = {}                        # mint -> Coin, insertion order
pending_buys = {}                 # client_id -> mint
pending_sells = {}                # client_id -> (mint, "half" or "all")
order_counter = [0]


def next_client_id(prefix):
    order_counter[0] += 1
    return prefix + str(order_counter[0])


def on_hello(state):
    coins.clear()
    pending_buys.clear()
    pending_sells.clear()
    order_counter[0] = 0


def record_fill(batch):
    """Fold this batch's fills in: entry prices for filled buys, exit
    state for filled sells. A rejected order changes nothing, so the next
    batch retries the same decision."""
    for fill in batch["fills"]:
        cid = fill["client_id"]
        if fill["side"] == "buy":
            mint = pending_buys.pop(cid, None)
            if fill["status"] != "filled" or mint is None:
                continue
            coin = coins.get(mint)
            tokens = int(fill["token_amount"])
            quote = int(fill["quote_lamports"])
            if coin is not None and tokens > 0:
                coin.entered = True
                coin.entry_price = quote / tokens
                coin.peak = coin.entry_price
                coin.trades_at_entry = coin.trades
        else:
            info = pending_sells.pop(cid, None)
            if fill["status"] != "filled" or info is None:
                continue
            mint, kind = info
            coin = coins.get(mint)
            if coin is None:
                continue
            if kind == "half":
                coin.tp1_done = True
            else:
                coin.done = True


def update_coin(event):
    """Fold one event into its coin's stats. Returns the mint."""
    mint = event["mint"]
    kind = event["event"]
    coin = coins.get(mint)
    if kind == "CreateEvent":
        first = int(event["virtual_quote_reserves"]) / int(event["virtual_token_reserves"])
        if coin is None:
            coin = coins[mint] = Coin(first)
        return mint
    if coin is None:
        first = int(event["virtual_quote_reserves"]) / int(event["virtual_token_reserves"])
        coin = coins[mint] = Coin(first)
    if kind == "TradeEvent":
        coin.trades += 1
        sol = int(event["sol_amount"])
        is_buy = bool(event["is_buy"])
        coin.recent.append((is_buy, sol))
        if is_buy:
            coin.buy_vol += sol
        else:
            coin.sell_vol += sol
        if len(coin.recent) > RECENT_TRADES:
            old_buy, old_sol = coin.recent.pop(0)
            if old_buy:
                coin.buy_vol -= old_sol
            else:
                coin.sell_vol -= old_sol
    elif kind == "CompleteEvent":
        coin.done = True
    return mint


def buy_share_bps(coin):
    total = coin.buy_vol + coin.sell_vol
    if total <= 0:
        return 0
    return coin.buy_vol * 10000 // total


def decide_exits(state, orders):
    """Sell orders for held positions. Returns nothing, appends to orders."""
    caps = state.hello["caps"]
    for mint, coin in coins.items():
        if not coin.entered or coin.done:
            continue
        if any(p.order["mint"] == mint for p in state.pending.values()):
            continue
        holding = state.positions.get(mint)
        if holding is None or holding.token_amount <= 0:
            continue
        curve = state.curves.get(mint)
        if curve is None or curve.complete:
            continue
        price = price_of(curve)
        if price <= 0.0 or coin.entry_price <= 0.0:
            continue
        if price > coin.peak:
            coin.peak = price
        entry = coin.entry_price
        sell_all = False
        sell_half = False
        if price * 10000 <= entry * (10000 - STOP_BPS):
            sell_all = True
        elif price * 10000 >= entry * (10000 + TP2_BPS):
            sell_all = True
        elif coin.tp1_done and price * 10000 <= coin.peak * (10000 - TRAIL_BPS):
            sell_all = True
        elif (
            coin.trades - coin.trades_at_entry >= TIME_EXIT_TRADES
            and price * 10000 < entry * (10000 + TIME_EXIT_BPS)
        ):
            sell_all = True
        elif not coin.tp1_done and price * 10000 >= entry * (10000 + TP1_BPS):
            sell_half = True
        if not sell_all and not sell_half:
            continue
        if len(orders) >= caps["max_orders_per_batch"]:
            return
        if sell_all:
            amount = holding.token_amount
            kind = "all"
        else:
            amount = holding.token_amount // 2
            if amount <= 0:
                continue
            kind = "half"
        cid = next_client_id("s")
        pending_sells[cid] = (mint, kind)
        orders.append(
            {
                "side": "sell",
                "client_id": cid,
                "mint": mint,
                "token_amount": amount,
                "max_slippage_bps": MAX_SLIPPAGE_BPS,
            }
        )


def decide_entries(state, touched, orders):
    """Buy orders for coins that just broke out. Appends to orders."""
    caps = state.hello["caps"]
    tx = state.tx_fee_lamports()
    free = state.free_cash_lamports()
    for mint in touched:
        if len(orders) >= caps["max_orders_per_batch"]:
            return
        coin = coins.get(mint)
        if coin is None or coin.done or coin.entered:
            continue
        if coin.trades < ENTRY_MIN_TRADES or coin.trades > ENTRY_MAX_TRADES:
            continue
        if buy_share_bps(coin) < ENTRY_BUY_SHARE_BPS:
            continue
        curve = state.curves.get(mint)
        if curve is None or curve.complete:
            continue
        if curve.real_quote_reserves < ENTRY_MIN_REAL_SOL:
            continue
        price = price_of(curve)
        if price <= 0.0 or coin.first_price <= 0.0:
            continue
        if price * 10000 < coin.first_price * (10000 + ENTRY_RISE_BPS):
            continue
        if mint in state.positions:
            continue
        if any(p.order["mint"] == mint for p in state.pending.values()):
            continue
        if state.committed_position_count() >= caps["max_open_positions"]:
            continue
        cap = state.max_buy_lamports(mint)
        amount = min(cap, STAKE_LAMPORTS)
        if amount < MIN_BUY_LAMPORTS:
            continue
        if free < amount + tx:
            continue
        cid = next_client_id("b")
        pending_buys[cid] = mint
        orders.append(
            {
                "side": "buy",
                "client_id": cid,
                "mint": mint,
                "quote_lamports": amount,
                "max_slippage_bps": MAX_SLIPPAGE_BPS,
            }
        )
        free -= amount + tx


def on_batch(state, batch):
    record_fill(batch)
    touched = []
    seen = set()
    for event in batch["events"]:
        mint = update_coin(event)
        if mint not in seen:
            seen.add(mint)
            touched.append(mint)
    orders = []
    decide_exits(state, orders)
    decide_entries(state, touched, orders)
    return orders


def on_done(state, done):
    return None


if __name__ == "__main__":
    makefun_bot.run(on_batch, on_hello=on_hello, on_done=on_done)
