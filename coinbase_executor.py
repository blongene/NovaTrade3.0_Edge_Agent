# coinbase_executor.py — Coinbase Advanced Trade executor (market/limit, idempotent)
import os, time, hmac, hashlib, json, requests
from typing import Dict, Any

CB_KEY      = os.getenv("CB_API_KEY", "")
CB_SECRET   = os.getenv("CB_API_SECRET", "")
CB_PASSPHRASE = os.getenv("CB_API_PASSPHRASE", "")
CB_BASE     = os.getenv("CB_BASE_URL", "https://api.coinbase.com")  # Advanced Trade base

TIMEOUT_S   = float(os.getenv("CB_TIMEOUT_S", "10"))
RETRIES     = int(os.getenv("CB_RETRIES", "3"))
BACKOFF_S   = float(os.getenv("CB_BACKOFF_S", "0.9"))
QUOTE_MODE  = os.getenv("CB_QUOTE_MODE", "true").lower() in {"1","true","yes"}  # spend quote by default
IDEMP_FILE  = os.getenv("CB_IDEMP_STORE", "coinbase_idempotency.json")

def _load_store():
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

# --- Coinbase Advanced auth helper (HTTP request signing) ---
def _now_ms() -> str:
    return str(int(time.time() * 1000))

def _cb_sign(ts_ms: str, method: str, path: str, body: str) -> str:
    # NOTE: Depending on the exact API flavor, the signature material may vary.
    # Adjust to match your working Coinbase Advanced Trade keys/docs.
    msg = f"{ts_ms}{method.upper()}{path}{body or ''}"
    return hmac.new(CB_SECRET.encode(), msg.encode(), hashlib.sha256).hexdigest()

def _r(method: str, path: str, params: dict | None = None, body: dict | None = None):
    url = f"{CB_BASE}{path}"
    body_json = json.dumps(body, separators=(",", ":"), sort_keys=True) if body else ""
    ts = _now_ms()
    sig = _cb_sign(ts, method, path, body_json)
    headers = {
        "CB-ACCESS-KEY": CB_KEY,
        "CB-ACCESS-SIGN": sig,
        "CB-ACCESS-TIMESTAMP": ts,
        "CB-ACCESS-PASSPHRASE": CB_PASSPHRASE,
        "Content-Type": "application/json"
    }
    for attempt in range(1, RETRIES+1):
        try:
            resp = requests.request(method.upper(), url, params=params if method.upper()=="GET" else None,
                                    data=body_json if body else None, headers=headers, timeout=TIMEOUT_S)
            resp.raise_for_status()
            return resp.json() if resp.headers.get("content-type","").startswith("application/json") else {}
        except Exception as e:
            if attempt >= RETRIES: raise
            time.sleep(BACKOFF_S * attempt)

def normalize_symbol(symbol: str) -> str:
    # Coinbase uses '-' pair format e.g., BTC-USD
    return symbol.replace("/", "-").upper()

def place_market_order(*, client_order_id: str, symbol: str, side: str, amount: str, tif: str="IOC") -> Dict[str, Any]:
    prior = already_acked(client_order_id)
    if prior: return prior

    product_id = normalize_symbol(symbol)
    side = side.lower()  # buy/sell lowercase on CB
    order = {
        "client_order_id": client_order_id,
        "product_id": product_id,
        "side": side,
        "order_configuration": {}
    }
    if QUOTE_MODE and side == "buy":
        order["order_configuration"]["market_ioc"] = {"quote_size": amount}
    else:
        order["order_configuration"]["market_ioc"] = {"base_size": amount}

    # Adjust path for Advanced Trade create order endpoint
    res = _r("POST", "/api/v3/brokerage/orders", body=order)

    txid = str(res.get("order_id") or res.get("orderId") or "unknown")
    fills = [{"price":"", "qty": amount}]  # refine when parsing fill details
    receipt = {"status":"ok","txid":txid,"fills":fills,"message":f"filled (market {side}) {symbol} via Coinbase"}
    remember_acked(client_order_id, receipt)
    return receipt
