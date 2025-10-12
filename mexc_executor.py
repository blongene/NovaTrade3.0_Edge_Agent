# mexc_executor.py — NovaTrade MEXC spot executor (idempotent + retry-safe)
import os, time, hmac, hashlib, requests, json
from typing import Dict, Any, Optional

MEXC_KEY    = os.getenv("MEXC_KEY", "")
MEXC_SECRET = os.getenv("MEXC_SECRET", "")
MEXC_BASE   = os.getenv("MEXC_BASE_URL", "https://api.mexc.com")
TIMEOUT_S   = float(os.getenv("MEXC_TIMEOUT_S", "10"))
RETRIES     = int(os.getenv("MEXC_RETRIES", "3"))
BACKOFF_S   = float(os.getenv("MEXC_BACKOFF_S", "0.9"))
QUOTE_MODE  = os.getenv("MEXC_QUOTE_MODE", "true").lower() in {"1","true","yes"}
IDEMP_FILE  = os.getenv("IDEMP_STORE", "mexc_idempotency.json")

def _load_store() -> dict:
    try:
        with open(IDEMP_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def _save_store(d: dict):
    try:
        with open(IDEMP_FILE, "w", encoding="utf-8") as f:
            json.dump(d, f, separators=(",", ":"), ensure_ascii=False)
    except Exception:
        pass

def already_acked(client_order_id: str):
    return _load_store().get(client_order_id)

def remember_acked(client_order_id: str, receipt: dict):
    d = _load_store()
    d[client_order_id] = receipt
    _save_store(d)

def _ts_ms() -> str:
    return str(int(time.time() * 1000))

def _sign(query: str) -> str:
    return hmac.new(MEXC_SECRET.encode(), query.encode(), hashlib.sha256).hexdigest()

def _headers() -> dict:
    return {"X-MEXC-APIKEY": MEXC_KEY, "Content-Type": "application/json"}

def _r(method: str, path: str, params: dict | None = None, body: dict | None = None, signed: bool = False):
    url = f"{MEXC_BASE}{path}"
    params = (params or {}).copy()
    if signed:
        params["timestamp"] = _ts_ms()
        qs = "&".join([f"{k}={params[k]}" for k in sorted(params)])
        params["signature"] = _sign(qs)
    for attempt in range(1, RETRIES+1):
        try:
            resp = requests.request(method.upper(), url,
                                    params=params if method.upper() in {"GET","DELETE"} else None,
                                    json=body if method.upper() in {"POST","PUT"} else None,
                                    headers=_headers(), timeout=TIMEOUT_S)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            if attempt >= RETRIES: raise
            time.sleep(BACKOFF_S * attempt)

def normalize_symbol(symbol: str) -> str:
    return symbol.replace("/", "").upper()

def place_market_order(*, client_order_id: str, symbol: str, side: str, amount: str, tif: str="IOC") -> Dict[str, Any]:
    prior = already_acked(client_order_id)
    if prior: return prior

    sym = normalize_symbol(symbol)
    side = side.upper()

    payload = {"symbol": sym, "side": side, "type": "MARKET", "clientOrderId": client_order_id}
    if QUOTE_MODE:
        payload["quoteOrderQty"] = amount
    else:
        payload["quantity"] = amount
    if tif: payload["timeInForce"] = tif

    # Adjust to your proven MEXC endpoint
    res = _r("POST", "/api/v3/order", params=payload, signed=True)

    txid = str(res.get("orderId") or res.get("data") or res.get("transactTime") or "unknown")
    fills = []
    if "fills" in res and isinstance(res["fills"], list):
        for f in res["fills"]:
            fills.append({
                "price": str(f.get("price","")),
                "qty":   str(f.get("qty","")),
                "fee":   str(f.get("commission","")),
                "feeAsset": str(f.get("commissionAsset","")),
            })
    else:
        fills.append({"price":"", "qty": amount})

    receipt = {"status":"ok","txid":txid,"fills":fills,"message":f"filled (market {side}) {symbol} via MEXC"}
    remember_acked(client_order_id, receipt)
    return receipt
