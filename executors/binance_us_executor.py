# executors/binance_us_executor.py
import os, time, hmac, hashlib, requests, urllib.parse

BUS_BASE = os.getenv("BINANCEUS_BASE_URL", "https://api.binance.us").rstrip("/")
TIMEOUT_S = int(os.getenv("BINANCEUS_TIMEOUT_S", "20"))

class BinanceUS:
    def __init__(self):
        self.key = os.getenv("BINANCEUS_API_KEY","").strip()
        self.sec = os.getenv("BINANCEUS_API_SECRET","").strip()
        self.base = BUS_BASE
        self.sess = requests.Session()
        self.sess.headers.update({"User-Agent":"NovaTrade-Edge-BinanceUS/1.1","X-MBX-APIKEY": self.key or ""})

    def _ts(self): return int(time.time()*1000)
    def _sign(self, qs:str): return hmac.new(self.sec.encode(), qs.encode(), hashlib.sha256).hexdigest()

    def _post_retry(self, path:str, params:dict, tries=3):
        p = dict(params or {})
        p.setdefault("timestamp", self._ts()); p.setdefault("recvWindow", 5000)
        for i in range(tries):
            qs = urllib.parse.urlencode(p, doseq=True)
            url = f"{self.base}{path}?{qs}&signature={self._sign(qs)}"
            r = self.sess.post(url, timeout=TIMEOUT_S)
            if r.status_code < 500 and r.status_code != 429:
                return r
            time.sleep(1.5 * (i + 1))
        return r

    def place_market(self, *, symbol:str, side:str, quote_qty:float=None, base_qty:float=None, newClientOrderId:str=None):
        params = {"symbol": symbol.upper(), "side": side.upper(), "type": "MARKET"}
        if side.upper()=="BUY":
            if quote_qty is None: return None, {"error":"BUY requires quote_qty"}
            params["quoteOrderQty"] = f"{float(quote_qty):.8f}"
        else:
            if base_qty is None: return None, {"error":"SELL requires base_qty"}
            params["quantity"] = f"{float(base_qty):.8f}"
        if newClientOrderId: params["newClientOrderId"] = str(newClientOrderId)
        r = self._post_retry("/api/v3/order", params)
        try: data = r.json()
        except Exception: data = {"raw": r.text}
        return r, data

def execute_market_order(*, venue_symbol: str, side: str,
                         amount_quote: float = 0.0, amount_base: float = 0.0,
                         client_id: str = "", edge_mode: str = "dryrun",
                         edge_hold: bool = False, **_):
    symbol = venue_symbol.replace("-","").replace("/","").upper()
    if edge_hold:
        return {"status":"held","message":"EDGE_HOLD enabled","fills":[],"venue":"BINANCEUS","symbol":symbol,"side":side}
    if edge_mode!="live":
        px = 60000.0
        qty = round((float(amount_quote or 0)/px) if side.upper()=="BUY" else float(amount_base or 0), 8)
        return {"status":"ok","txid":f"SIM-BUS-{int(time.time()*1000)}","fills":[{"qty":qty,"price":px}],
                "venue":"BINANCEUS","symbol":symbol,"side":side,"executed_qty":qty,"avg_price":px,
                "message":"binanceus dryrun simulated fill"}
    if not (os.getenv("BINANCEUS_API_KEY") and os.getenv("BINANCEUS_API_SECRET")):
        return {"status":"error","message":"Missing BINANCEUS_API_KEY/SECRET","fills":[],
                "venue":"BINANCEUS","symbol":symbol,"side":side}
    bus = BinanceUS()
    resp, data = (bus.place_market(symbol=symbol, side="BUY",  quote_qty=float(amount_quote or 0), base_qty=None, newClientOrderId=str(client_id))
                  if side.upper()=="BUY" else
                  bus.place_market(symbol=symbol, side="SELL", quote_qty=None, base_qty=float(amount_base or 0), newClientOrderId=str(client_id)))
    if resp is None:
        return {"status":"error","message":data.get("error","bad params"),"fills":[],"venue":"BINANCEUS","symbol":symbol,"side":side}
    if resp.status_code >= 400:
        return {"status":"error","message":f"{resp.status_code} {data}","fills":[],"venue":"BINANCEUS","symbol":symbol,"side":side}
    order_id = data.get("orderId") or ""
    fills=[]
    if isinstance(data.get("fills"), list):
        for f in data["fills"]:
            try: fills.append({"price": float(f.get("price",0)), "qty": float(f.get("qty",0))})
            except Exception: pass
    return {"status":"ok","txid": str(order_id) or f"BUS-NOORD-{int(time.time()*1000)}","fills":fills,
            "venue":"BINANCEUS","symbol":symbol,"side":side,
            "message":"binanceus live order accepted" if order_id else "binanceus response parsed"}
