# edge_pretrade.py — unified venue filters + preflight routing for BTC pairs
from __future__ import annotations
from typing import Dict, Tuple

# ---------------------------------------------------------------------------
# Filters: min base qty & min quote notional (USD/USDC/USDT)
# Tweak if your venue minimums change.
# ---------------------------------------------------------------------------
FILTERS = {
    "KRAKEN": {
        ("BTC","USD"):  {"pair":"XBTUSD",  "min_qty":0.00005, "min_notional":25.0},
        ("BTC","USDT"): {"pair":"XBTUSDT", "min_qty":0.00005, "min_notional":25.0},
    },
    "COINBASE": {
        # Coinbase Advanced commonly supports BTC-USD and BTC-USDC.
        ("BTC","USD"):  {"product":"BTC-USD",  "min_qty":0.000001, "min_notional":2.0},
        ("BTC","USDC"): {"product":"BTC-USDC", "min_qty":0.000001, "min_notional":2.0},
    },
    "BINANCEUS": {
        # BinanceUS typical minimum notional ≈ $10
        ("BTC","USDT"): {"symbol":"BTCUSDT", "min_qty":0.00001, "min_notional":10.0},
        ("BTC","USD"):  {"symbol":"BTCUSD",  "min_qty":0.00001, "min_notional":10.0},
    },
}

def _pick_quote_by_balance(balances: Dict[str,float], candidates: Tuple[str, ...], min_notional: float) -> str:
    """
    Choose the first quote in `candidates` for which wallet balance >= min_notional.
    If none match, return the richest candidate to produce a precise denial.
    """
    richest = candidates[0]
    richest_amt = -1.0
    for q in candidates:
        have = float(balances.get(q, 0.0))
        if have >= min_notional:
            return q
        if have > richest_amt:
            richest_amt = have
            richest = q
    return richest

def pick_smart_quote(venue: str, balances: Dict[str,float], base: str, quote: str) -> str:
    """
    For venues that list multiple quotes for BTC (e.g., USD vs USDC/USDT),
    pick the quote based on wallet balance and minimum notional.
    If `quote` is already an explicit supported one, keep it.
    """
    v = venue.upper()
    b = base.upper()
    q = quote.upper()

    # Candidate quotes available for this base on this venue
    candidates = tuple(sorted({qq for (bb, qq) in FILTERS.get(v, {}).keys() if bb == b}))
    if not candidates:
        return q or quote  # venue unknown -> keep user's symbol as-is

    if q in candidates:
        return q  # user fixed it explicitly

    # Fall back to a smart choice using the highest min_notional among candidates
    # (safe side—if you pass this, you pass all; or denial message is clear)
    min_req = max(FILTERS[v][(b, qq)]["min_notional"] for qq in candidates)
    return _pick_quote_by_balance(balances or {}, candidates, min_req)

def pretrade_validate(
    venue: str,
    base: str,
    quote: str,
    price: float,
    amount_base: float,
    amount_quote: float,
    venue_balances: Dict[str,float],
) -> Tuple[bool,str,str,float,float]:
    """
    Returns (ok, reason, chosen_quote, need_min_qty, need_min_notional).

    - Auto-selects quote for venues offering multiple (COINBASE: USD/USDC, BINANCEUS: USD/USDT, KRAKEN: USD/USDT).
    - Validates min base qty and min quote notional vs venue rules.
    - Validates wallet balance for quote when spending in quote terms (BUY).
    """
    v = venue.upper()
    b = base.upper()
    q_in = (quote or "").upper()

    # Smart quote (if user didn't hardcode a supported one)
    q = pick_smart_quote(v, venue_balances or {}, b, q_in)

    f = FILTERS.get(v, {}).get((b, q))
    if not f:
        # Unknown pair config—fail open (venue will validate), but still return chosen quote
        return True, "no_filter", q, 0.0, 0.0

    min_qty       = float(f["min_qty"])
    min_notional  = float(f["min_notional"])

    # Notional math
    if amount_base and amount_base > 0:
        base_qty = float(amount_base)
        notional = base_qty * float(price or 0.0)
    else:
        # Spending in quote (BUY $amount)
        if price is None or price <= 0:
            return False, "no_price", q, min_qty, min_notional
        base_qty = float(amount_quote) / float(price)
        notional = float(amount_quote)

    if base_qty < min_qty:
        return False, f"min volume {min_qty:g} not met", q, min_qty, min_notional

    if notional < min_notional:
        return False, f"min notional {min_notional:g} {q} not met", q, min_qty, min_notional

    # Check wallet availability when BUY spending in quote terms
    if amount_quote and amount_quote > 0:
        have = float(venue_balances.get(q, 0.0))
        if have + 1e-9 < float(amount_quote):
            return False, f"insufficient {q}: have {have:.2f}, need {float(amount_quote):.2f}", q, min_qty, min_notional

    return True, "ok", q, min_qty, min_notional
