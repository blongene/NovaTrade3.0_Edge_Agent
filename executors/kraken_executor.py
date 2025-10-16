#!/usr/bin/env python3
# executors/kraken_executor.py — MARKET executor with min-volume check and free-balance clamp.
import os, time, hmac, hashlib, base64, urllib.parse, requests
from datetime import datetime, timezone
from typing import Dict, Any

BASE = os.getenv("KRAKEN_BASE_URL", "https://api.kraken.com").rstrip("/")
KEY  = os.getenv("KRAKEN_KEY", "")
SEC  = os.getenv("KRAKEN_SECRET", "")  # base64 from Kraken
EDGE_MODE = (os.getenv("EDGE_MODE") or "dryrun").lower()
TIMEOUT = 15

def _sym(venue_symbol: str) -> str:
    # BTC/USDT -> XBTUSDT
    s = (venue_symbol or "BTC/USDT").upper().replace("/", "")
    s = s.replace("BTC", "XBT")
    return s

def _public(path, params=None):
    r = requests.get(f"{BASE}{path}", params=params or {}, timeout=TIMEOUT)
    r.raise_for_status(); return r.json()

def _pair_info(pair: str) -> dict:
    try:
        j = _public("/0/public/AssetPairs", {"pair": pair})
        return next(iter(j["result"].values()))
    except Exception:
        return {}

def _ticker_price(pair: str) -> float:
    j = _public("/0/public/Ticker", {"pair": pair})
    result = next(iter(j["result"].values()))
    return float(result["c"][0])

def _sign(path: str, data: dict) -> dict:
    nonce = str(int(time.time() * 1000))
    data = {**data, "nonce": nonce}
    post = urllib.parse.urlencode(data)
    sha256 = hashlib.sha256((nonce + post).encode()).digest()
    sig = base64.b64encode(hmac.new(base64.b64decode(SEC), path.encode() + sha256, hashlib.sha512).digest()).decode()
    return {"hdr": {"API-Key": KEY, "API-Sign": sig}, "qs": post}

def _private(path: str, data: dict):
    s = _sign(path, data)
    r = requests.post(f"{BASE}{path}", data=s["qs"], headers=s["hdr"], timeout=TIMEOUT)
    j = r.json()
    if j.get("error"): raise RuntimeError(",".join(j["error"]))
    return j["result"]

def _balance() -> Dict[str, float]:
    """Return balances with Kraken symbols (e.g., XBT, USDT)."""
    s = _sign("/0/private/Balance", {})
    r = requests.post(f"{BASE}/0/private/Balance", data=s["qs"], headers=s["hdr"], timeout=TIMEOUT)
    j = r.json()
    if j.get("error"): raise RuntimeError(",".join(j["error"]))
    out = {}
    for k, v in (j.get("result") or {}).items():
        try: out[k.upper()] = float(v)
        except Exception: pass
    return out

def execute_market_order(*, venue_symbol: str, side: str,
                         amount_quote: float = 0.0, amount_base: float = 0.0,
                         client_id: str = "", edge_mode: str = "dryrun",
                         edge_hold: bool = False, **_):
    pair = _sym(venue_symbol)          # e.g., XBTUSDT
    side_uc = (side or "").upper()

    if edge_hold:
        return {"status":"held","message":"EDGE_HOLD enabled","fills":[],"venue":"KRAKEN","symbol":pair,"side":side_uc}

    # DRYRUN
    if edge_mode != "live":
        px = 60000.0
        qty = round((float(amount_quote or 0)/px) if side_uc=="BUY" else float(amount_base or 0), 8)
        return {"status":"ok","txid":f"SIM-KR-{int(time.time()*1000)}","fills":[{"qty":qty,"price":px}],
                "venue":"KRAKEN","symbol":pair,"side":side_uc,"executed_qty":qty,"avg_price":px,
                "message":"kraken dryrun simulated fill"}

    if not (KEY and SEC):
        return {"status":"error","message":"Missing KRAKEN_KEY/KRAKEN_SECRET","fills":[],
                "venue":"KRAKEN","symbol":pair,"side":side_uc}

    # min volume guard
    info = _pair_info(pair)
    default_min = 0.0001 if pair.startswith("XBT") else 0.0
    try: ordermin = float(info.get("ordermin", default_min) or default_min)
    except Exception: ordermin = default_min

    px = 0.0
    try: px = _ticker_price(pair)
    except Exception: px = 0.0

    # SELL clamp to free balance (Kraken uses XBT)
    if side_uc == "SELL":
        try:
            bals = _balance()
            free = float(bals.get("XBT", 0.0))
        except Exception:
            free = 0.0
        qty_req = float(amount_base or 0.0)
        qty = round(min(max(0.0, qty_req), max(0.0, free - 1e-8)), 8)
        if qty < ordermin:
            return {"status":"error","message":f"qty {qty:.8f} < ordermin {ordermin:.8f}","fills":[],
                    "venue":"KRAKEN","symbol":pair,"side":side_uc}
    else:
        if not amount_quote or float(amount_quote) <= 0:
            return {"status":"error","message":"BUY requires amount_quote > 0","fills":[],
                    "venue":"KRAKEN","symbol":pair,"side":side_uc}
        qty = round((float(amount_quote) / (px or 1.0)), 8)
        if qty < ordermin:
            return {"status":"error","message":f"min volume {ordermin:.8f} not met","fills":[],
                    "venue":"KRAKEN","symbol":pair,"side":side_uc}

    # LIVE order
    res = _private("/0/private/AddOrder",
                   {"pair": pair, "type":"buy" if side_uc=="BUY" else "sell",
                    "ordertype":"market","volume": f"{qty:.8f}"})
    txid = (res.get("txid") or [None])[0]
    return {"status":"ok","txid":txid or f"KR-NOORD-{int(time.time()*1000)}","fills":[],
            "venue":"KRAKEN","symbol":pair,"side":side_uc,
            "message":"kraken live order accepted" if txid else "kraken response parsed"}
