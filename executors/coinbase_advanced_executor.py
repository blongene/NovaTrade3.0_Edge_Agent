# executors/coinbase_advanced_executor.py
import os, time, json, base64, hmac, hashlib, requests

CB_BASE = os.getenv("COINBASE_BASE_URL", "https://api.coinbase.com").rstrip("/")
ORDERS_PATH  = "/api/v3/brokerage/orders"
TIMEOUT_S    = int(os.getenv("COINBASE_TIMEOUT_S", "20"))

class CoinbaseAdv:
    def __init__(self):
        self.key   = os.getenv("COINBASE_API_KEY", "").strip()
        self.secret_b64 = os.getenv("COINBASE_API_SECRET", "").strip()
        self.passphrase = os.getenv("COINBASE_API_PASSPHRASE", "").strip()  # optional
        self.base  = CB_BASE
        self.sess  = requests.Session()
        self.sess.headers.update({"User-Agent": "NovaTrade-Edge-CBAdv/1.1"})

    def _ts(self) -> str:  # seconds
        return str(int(time.time()))

    def _sign(self, ts: str, method: str, path: str, body: str) -> str:
        secret = base64.b64decode(self.secret_b64)
        msg = (ts + method.upper() + path + (body or "")).encode("utf-8")
        return base64.b64encode(hmac.new(secret, msg, hashlib.sha256).digest()).decode()

    def _hdrs(self, ts: str, method: str, path: str, body: str):
        h = {
            "CB-ACCESS-KEY": self.key,
            "CB-ACCESS-TIMESTAMP": ts,
            "CB-ACCESS-SIGN": self._sign(ts, method, path, body),
            "Content-Type": "application/json",
        }
        if self.passphrase:
            h["CB-ACCESS-PASSPHRASE"] = self.passphrase
        return h

    def _post_retry(self, path: str, body: dict, tries=3):
        raw = json.dumps(body, separators=(",", ":"), sort_keys=True)
        for i in range(tries):
            ts = self._ts()
            r = self.sess.post(self.base + path, data=raw, headers=self._hdrs(ts, "POST", path, raw), timeout=TIMEOUT_S)
            if r.status_code < 500:
                return r
            time.sleep(1.5 * (i + 1))
        return r

    def place_market(self, *, product_id: str, side: str, quote_size: float, client_order_id: str):
        body = {
            "client_order_id": str(client_order_id),
            "product_id": product_id,
            "side": side.lower(),
            "order_configuration": {"market_market_ioc": {"quote_size": str(quote_size)}}
        }
        return self._post_retry(ORDERS_PATH, body)

def execute_market_order(*, venue_symbol: str, side: str,
                         amount_quote: float = 0.0, amount_base: float = 0.0,
                         client_id: str = "", edge_mode: str = "dryrun", edge_hold: bool = False, **_):
    symbol = venue_symbol.replace("/", "-").upper()  # BTC/USDC → BTC-USDC
    if edge_mode == "live" and side.upper() == "SELL" and not amount_base:
        return {"status":"error","message":"Coinbase MARKET SELL requires base amount (amount_base)","fills":[],
                "venue":"COINBASE","symbol":symbol,"side":side}
    if edge_hold:
        return {"status":"held","message":"EDGE_HOLD enabled","fills":[],"venue":"COINBASE","symbol":symbol,"side":side}
    if edge_mode != "live":
        px = 60000.0
        qty = round(float(amount_quote or 0)/px, 8)
        return {"status":"ok","txid":f"SIM-CB-{int(time.time()*1000)}","fills":[{"qty":qty,"price":px}],
                "venue":"COINBASE","symbol":symbol,"side":side,"executed_qty":qty,"avg_price":px,
                "message":"coinbase dryrun simulated fill"}
    if not (os.getenv("COINBASE_API_KEY") and os.getenv("COINBASE_API_SECRET")):
        return {"status":"error","message":"Missing COINBASE_API_KEY/COINBASE_API_SECRET","fills":[],
                "venue":"COINBASE","symbol":symbol,"side":side}
    cb = CoinbaseAdv()
    resp = cb.place_market(product_id=symbol, side=side, quote_size=float(amount_quote or 0), client_order_id=str(client_id))
    try: data = resp.json()
    except Exception: data = {"raw": resp.text}
    if resp.status_code >= 400:
        return {"status":"error","message":f"{resp.status_code} {data}","fills":[],"venue":"COINBASE","symbol":symbol,"side":side}
    order_id = (data.get("success_response") or {}).get("order_id") or data.get("order_id") or ""
    return {"status":"ok","txid":order_id or f"CB-NOORD-{int(time.time()*1000)}","fills":data.get("fills") or [],
            "venue":"COINBASE","symbol":symbol,"side":side,
            "message":"coinbase live order accepted" if order_id else "coinbase response parsed"}
