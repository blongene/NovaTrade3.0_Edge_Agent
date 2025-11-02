# edge_pretrade.py — venue filters + preflight routing
from __future__ import annotations
import math
from typing import Dict, Tuple

# Static filters (tweak if venues change): min base qty and min quote notional in quote currency
FILTERS = {
    "KRAKEN": {
        # Kraken uses XBT for BTC internally
        ("BTC","USD"):  {"pair":"XBTUSD",  "min_qty":0.00005, "min_notional":25.0},
        ("BTC","USDT"): {"pair":"XBTUSDT", "min_qty":0.00005, "min_notional":25.0},
    },
    "COINBASE": {
        ("BTC","USD"):  {"product":"BTC-USD", "min_qty":0.000001, "min_notional":2.0},  # generous
    },
    "BINANCEUS": {
        ("BTC","USDT"): {"symbol":"BTCUSDT", "min_qty":0.00001, "min_notional":5.0},
        ("BTC","USD"):  {"symbol":"BTCUSD",  "min_qty":0.00001, "min_notional":5.0},
    },
}

def _round_up(x: float, step: float) -> float:
    if step <= 0: return x
    return math.ceil(x/step)*step

def pick_kraken_quote(balances: Dict[str,float]) -> str:
    """Choose USD or USDT for Kraken based on wallet ≥ $25."""
    usd  = float(balances.get("USD" , 0.0))
    usdt = float(balances.get("USDT", 0.0))
    # Prefer the side that already satisfies min-notional
    if usd  >= 25.0: return "USD"
    if usdt >= 25.0: return "USDT"
    # Otherwise pick the richer side so we can return a precise denial
    return "USDT" if usdt >= usd else "USD"

def pretrade_validate(venue: str, base: str, quote: str, price: float,
                      amount_base: float, amount_quote: float,
                      venue_balances: Dict[str,float]) -> Tuple[bool,str,str,float,float]:
    """
    Returns (ok, reason, chosen_quote, need_base, need_quote_notional).
    ok==True means it's safe to place.
    If ok==False, reason is a human-friendly denial.
    """
    v = venue.upper()
    b = base.upper()
    q = quote.upper()

    # Kraken: auto-pick USD/USDT by balance & min-notional, unless caller fixed quote explicitly
    if v == "KRAKEN" and q in ("", "USD", "USDT"):
        chosen = pick_kraken_quote(venue_balances or {})
        if quote == "" or quote in ("USD","USDT"):
            q = chosen

    f = FILTERS.get(v, {}).get((b,q))
    if not f:
        return True, "no_filter", q, 0.0, 0.0  # let venue handle

    min_qty = float(f["min_qty"])
    min_notional = float(f["min_notional"])

    # Decide how we're sizing: base or quote?
    notional = 0.0
    need_base_qty = 0.0

    if amount_base and amount_base > 0:
        need_base_qty = float(amount_base)
        notional = need_base_qty * price
    else:
        # amount_quote is the spender (BUY). Convert to base.
        if price <= 0:
            return False, "no_price", q, 0.0, min_notional
        need_base_qty = amount_quote / price
        notional = float(amount_quote)

    if need_base_qty < min_qty:
        return False, f"min volume {min_qty:g} not met", q, min_qty, min_notional

    if notional < min_notional:
        return False, f"min notional {min_notional:g} {q} not met", q, min_qty, min_notional

    # Also check wallet for the quote we plan to spend (BUY path)
    if amount_quote and amount_quote > 0:
        have = float(venue_balances.get(q, 0.0))
        if have + 1e-9 < amount_quote:
            return False, f"insufficient {q}: have {have:.2f}, need {amount_quote:.2f}", q, min_qty, min_notional

    return True, "ok", q, min_qty, min_notional
