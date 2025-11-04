
# edge_policy_aware_executor.py
from __future__ import annotations
import os, json
from typing import Any, Dict, Optional

EDGE_HOLD = os.getenv("EDGE_HOLD","0").lower() in ("1","true","yes")

def get_price_usd(venue: str, symbol: str):
    # stub; fill with your adapters if available
    return None

def get_quote_reserve_usd(by_venue_balances: Dict[str, Dict[str, float]], venue: str, symbol: str):
    try:
        v = (venue or "").upper()
        s = (symbol or "").upper().replace(":","").replace(".","")
        # naive split: BTC-USD / BTCUSDT -> ("BTC","USD/USDT")
        base, quote = (s.split("-",1)+["USD"])[:2] if "-" in s else (s[:-3], s[-3:]) if len(s) > 3 else (s, "USD")
        venue_bal = (by_venue_balances or {}).get(v) or {}
        q = float(venue_bal.get(quote, 0.0))
        if quote in ("USD","USDT","USDC"): return q
        return None
    except Exception:
        return None

def maybe_execute_command(command: Dict[str, Any], context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    ctx = context or {}
    payload = dict(command.get("payload") or {})
    venue  = (payload.get("venue") or "").upper()
    symbol = (payload.get("symbol") or "").upper()
    side   = (payload.get("side") or "").lower()
    try:
        amount = float(payload.get("amount") or 0.0)
    except Exception:
        amount = 0.0

    price_usd = payload.get("price_usd") or get_price_usd(venue, symbol)
    quote_reserve_usd = payload.get("quote_reserve_usd") or get_quote_reserve_usd(ctx.get("balances_by_venue") or {}, venue, symbol)

    observations = {"price_usd": price_usd, "quote_reserve_usd": quote_reserve_usd, "edge_hold": bool(EDGE_HOLD)}

    if EDGE_HOLD:
        return {"status":"skipped","reason":"EDGE_HOLD","applied":{"venue":venue,"symbol":symbol,"side":side,"amount":amount},"observations":observations}

    # PLACE ORDER HERE (your adapter) — for now, return dry-run ok:
    return {"status":"ok","reason":"executed(dry-run)","applied":{"venue":venue,"symbol":symbol,"side":side,"amount":amount},"observations":observations}
