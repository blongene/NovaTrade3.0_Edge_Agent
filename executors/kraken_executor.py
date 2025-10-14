#!/usr/bin/env python3
# kraken_executor.py — MARKET order with dryrun + min-volume precheck + adapter

import os, time, hmac, hashlib, base64, urllib.parse, requests
from datetime import datetime, timezone

BASE = os.getenv("KRAKEN_BASE_URL", "https://api.kraken.com").rstrip("/")
KEY  = os.getenv("KRAKEN_KEY", "")
SEC  = os.getenv("KRAKEN_SECRET", "")  # base64 from Kraken
EDGE_MODE = (os.getenv("EDGE_MODE") or "dryrun").lower()
TIMEOUT = 15

def _sym(s: str) -> str:
    return (s or "BTC/USDT").upper().replace("/", "").replace("BTC", "XBT")

def _public(path, params=None):
    r = requests.get(f"{BASE}{path}", params=params or {}, timeout=TIMEOUT)
    r.raise_for_status(); return r.json()

def _ticker_price(pair: str) -> float:
    j = _public("/0/public/Ticker", {"pair": pair})
    result = next(iter(j["result"].values()))
    return float(result["c"][0])

def _pair_info(pair: str) -> dict:
    try:
        j = _public("/0/public/AssetPairs", {"pair": pair})
        return next(iter(j["result"].values()))
    except Exception:
        return {}

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

def _kr_query_order(txid: str) -> dict:
    s = _sign("/0/private/QueryOrders", {"txid": txid})
    r = requests.post(f"{BASE}/0/private/QueryOrders", data=s["qs"], headers=s["hdr"], timeout=TIMEOUT)
    j = r.json()
    if j.get("error"): raise RuntimeError(",".join(j["error"]))
    return j["result"][txid]

def _normalize_kraken(txid: str, side_hint="BUY") -> dict:
    od = _kr_query_order(txid)
    vol  = float(od.get("vol_exec") or 0.0)
    cost = float(od.get("cost") or 0.0)
    fee  = float(od.get("fee") or 0.0)
    avg  = (cost / vol) if vol else 0.0
    pair = od.get("descr",{}).get("pair") or od.get("pair","XBTUSDT")
    ts   = datetime.fromtimestamp(float(od.get("opentm") or od.get("closetm") or time.time()), tz=timezone.utc).isoformat()
    return {
        "receipt_id": f"KRAKEN:{txid}",
        "venue": "KRAKEN",
        "symbol": pair,
        "side": side_hint.upper(),
        "executed_qty": f"{vol:.8f}",
        "avg_price": f"{avg:.8f}",
        "quote_spent": f"{cost:.8f}",
        "fee": f"{fee:.8f}",
        "fee_asset": "",
        "order_id": "",
        "txid": txid,
        "status": od.get("status",""),
        "timestamp_utc": ts,
    }

def execute(cmd: dict) -> dict:
    p = (cmd or {}).get("payload") or {}
    symbol = p.get("symbol") or "BTC/USDT"
    side   = (p.get("side") or "BUY").upper()
    quote  = float(p.get("quote_amount") or p.get("amount") or 0.0)
    pair   = _sym(symbol)

    try: price = _ticker_price(pair)
    except Exception as e:
        return {"ok": False, "venue": "KRAKEN", "pair": pair, "error": f"ticker: {e}"}

    info = _pair_info(pair)
    default_min = 0.0001 if pair.startswith("XBT") else 0.0
    try: ordermin = float(info.get("ordermin", default_min) or default_min)
    except Exception: ordermin = default_min

    qty = round((quote / price) if quote > 0 else float(p.get("base_amount") or 0.0), 8)
    if qty < ordermin:
        needed_quote = round((ordermin * price) * 1.02, 2)
        return {"ok": False, "venue": "KRAKEN", "pair": pair, "error": "min_volume",
                "ordermin_base": f"{ordermin:.8f}", "est_price": price, "your_qty": f"{qty:.8f}",
                "hint": f"Increase quote_amount to at least ~${needed_quote}"}

    if EDGE_MODE != "live":
        return {"ok": True, "simulated": True, "venue": "KRAKEN",
                "pair": pair, "est_price": price, "est_qty": qty, "note": "dryrun"}

    if not (KEY and SEC):
        return {"ok": False, "venue": "KRAKEN", "pair": pair, "error": "missing KRAKEN_KEY/KRAKEN_SECRET"}

    data = {"pair": pair, "type": "buy" if side=="BUY" else "sell", "ordertype": "market", "volume": f"{qty:.8f}"}
    res  = _private("/0/private/AddOrder", data)
    txid = (res.get("txid") or [None])[0]
    return {"ok": True, "venue":"KRAKEN","pair":pair,"txid":txid,"price":f"{price:.8f}","qty":f"{qty:.8f}","raw":res}

# --- Adapter so Edge can call with the same shape as other executors ---------
def execute_market_order(*, venue_symbol: str, side: str,
                         amount_quote: float = 0.0, amount_base: float = 0.0,
                         client_id: str = "", edge_mode: str = "dryrun",
                         edge_hold: bool = False, **_):
    pair = _sym(venue_symbol)
    if edge_hold:
        return {"status":"held","message":"EDGE_HOLD enabled","fills":[],"venue":"KRAKEN","symbol":pair,"side":side}
    if edge_mode!="live":
        px = _ticker_price(pair) if pair else 60000.0
        qty = round((float(amount_quote or 0)/px) if side.upper()=="BUY" else float(amount_base or 0), 8)
        return {"status":"ok","txid":f"SIM-KR-{int(time.time()*1000)}","fills":[{"qty":qty,"price":px}],
                "venue":"KRAKEN","symbol":pair,"side":side,"executed_qty":qty,"avg_price":px,
                "message":"kraken dryrun simulated fill"}
    if not (KEY and SEC):
        return {"status":"error","message":"Missing KRAKEN_KEY/KRAKEN_SECRET","fills":[],
                "venue":"KRAKEN","symbol":pair,"side":side}
    # live
    quote = float(amount_quote or 0.0)
    base  = float(amount_base or 0.0)
    qty   = base if (side.upper()=="SELL" and base>0) else (quote / (_ticker_price(pair) or 1))
    res = _private("/0/private/AddOrder",
                   {"pair": pair, "type":"buy" if side.upper()=="BUY" else "sell",
                    "ordertype":"market","volume": f"{qty:.8f}"})
    txid = (res.get("txid") or [None])[0]
    return {"status":"ok","txid":txid or f"KR-NOORD-{int(time.time()*1000)}","fills":[],
            "venue":"KRAKEN","symbol":pair,"side":side,"message":"kraken live order accepted" if txid else "kraken response parsed"}
