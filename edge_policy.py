
# edge_policy.py — Policy-lite pre-trade guard for NovaTrade Edge
# Reuses existing env:
#   QUOTE_FLOORS_JSON — e.g. {"BINANCEUS":{"USDT":10},"COINBASE":{"USDC":10},"KRAKEN":{"USDT":10}}
#   MIN_QUOTE_RESERVE_USD — global reserve not to breach (default 0)
#   ROUTER_DEFAULT_VENUE — used elsewhere; not required here
#
# Usage (in edge_agent.py BEFORE sending an order):
#   from edge_policy import enforce_pretrade
#   allowed, msg, fixed_amount = enforce_pretrade(venue, symbol, side, amount, wallet_quote_balances)
#   if not allowed:  # send error receipt
#       ...
#   amount = fixed_amount  # may be rounded up to meet venue minimums

from __future__ import annotations
import os, json, re, math

def _min_notional_map():
    """Defaults per venue; overlay with MIN_NOTIONAL_JSON env if provided."""
    try:
        extra = json.loads(os.getenv("MIN_NOTIONAL_JSON","") or "{}")
    except Exception:
        extra = {}
    base = {
        "BINANCEUS": {"USDT": 10.0},
        "COINBASE":  {"USDC": 10.0, "USD": 10.0},
        "KRAKEN":    {"USDT": 5.0,  "USD": 5.0},
    }
    for v, m in (extra or {}).items():
        base.setdefault(v.upper(), {}).update({k.upper(): float(vv) for k, vv in m.items()})
    return base

def _quote_floors_map():
    try:
        cfg = json.loads(os.getenv("QUOTE_FLOORS_JSON","") or "{}")
        return {v.upper(): {q.upper(): float(x) for q, x in d.items()} for v, d in cfg.items()}
    except Exception:
        return {}

def _split_symbol(symbol: str):
    """Accept BTC-USD or BTCUSD or BTC/USDT styles."""
    s = symbol.upper().replace("/", "-")
    if "-" in s:
        base, quote = s.split("-", 1)
    else:
        if s.endswith("USDT") or s.endswith("USDC") or s.endswith("BUSD"):
            base, quote = s[:-4], s[-4:]
        else:
            base, quote = s[:-3], s[-3:]
    return base, quote

def _precision_for(venue: str, symbol: str):
    """Heuristic precision; overlay via PRECISION_JSON env if desired."""
    try:
        prec = json.loads(os.getenv("PRECISION_JSON","") or "{}")
        vmap = prec.get(venue.upper(), {}).get(symbol.upper(), {})
        return int(vmap.get("qty", 6)), int(vmap.get("price", 2))
    except Exception:
        return 6, 2

def _round_qty(qty: float, places: int):
    """Round DOWN to avoid dust rejections."""
    factor = 10 ** places
    return math.floor(qty * factor) / factor

def enforce_pretrade(venue: str, symbol: str, side: str, amount_quote: float, wallet_quote_balances: dict):
    """
    Ensure we do not breach quote floors or min-notional.
    amount_quote is quote notional (e.g., 10 USDT).
    Returns (allowed: bool, message: str, adjusted_amount_quote: float)
    """
    venue_u = (venue or "").upper()
    side_u = (side or "").upper()
    base, quote = _split_symbol(symbol)
    floors = _quote_floors_map()
    mins = _min_notional_map()

    # 1) Quote floors / global reserve
    reserve = float(os.getenv("MIN_QUOTE_RESERVE_USD","0") or 0.0)
    floor = max(
        float(floors.get(venue_u, {}).get(quote, 0.0)),
        reserve if quote in ("USD","USDT","USDC") else 0.0
    )
    bal = float(wallet_quote_balances.get(quote, 0.0))

    if side_u == "BUY":
        if bal - amount_quote < floor:
            return False, f"quote floor {floor} {quote} would be breached (bal={bal:.2f}, spend={amount_quote:.2f})", amount_quote

    # 2) Venue min-notional
    min_req = float(mins.get(venue_u, {}).get(quote, 0.0))
    adj = amount_quote
    if adj < min_req:
        adj = min_req

    # 3) Rounding is qty-side; we keep amount in quote and let executor compute qty
    qty_places, _ = _precision_for(venue_u, f"{base}-{quote}")
    # (leave amount_quote untouched; executor rounds qty from quote/price)

    return True, "ok", adj
