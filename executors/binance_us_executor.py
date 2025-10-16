# executors/binance_us_executor.py
# Binance.US MARKET executor with lot-size clamp, min-notional check, and free-balance clamp.
import os, time, hmac, hashlib, requests, urllib.parse
from typing import Dict, Any

BUS_BASE = os.getenv("BINANCEUS_BASE_URL", "https://api.binance.us").rstrip("/")
TIMEOUT_S = int(os.getenv("BINANCEUS_TIMEOUT_S", "20"))

class BinanceUS:
    def __init__(self):
        self.key = os.getenv("BINANCEUS_API_KEY","").strip()
        self.sec = os.getenv("BINANCEUS_API_SECRET","").strip()
        self.base = BUS_BASE
        self.sess = requests.Session()
        self.sess.headers.update({"User-Agent":"NovaTrade-Edge-BinanceUS/1.3","X-MBX-APIKEY": self.key or ""})

    def _ts(self): return int(time.time()*1000)
    def _sign(self, qs:str): return hmac.new(self.sec.encode(), qs.encode(), hashlib.sha256).hexdigest()

    def _req(self, method:str, path:str, params:dict, tries=3):
        p = dict(params or {})
        p.setdefault("timestamp", self._ts()); p.setdefault("recvWindow", 5000)
        for i in range(tries):
            qs = urllib.parse.urlencode(p, doseq=True)
            url = f"{self.base}{path}?{qs}&signature={self._sign(qs)}"
            r = self.sess.request(method.upper(), url, timeout=TIMEOUT_S)
            if r.status_code < 500 and r.status_code != 429:
                return r
            time.sleep(1.5 * (i + 1))
        return r

    def account(self) -> Dict[str, Any]:
        r = self._req("GET", "/api/v3/account", {})
        try: return r.json()
        except Exception: return {}

    def exchange_info(self, symbol:str) -> Dict[str, Any]:
        r = requests.get(f"{self.base}/api/v3/exchangeInfo", params={"symbol": symbol}, timeout=TIMEOUT_S)
        try: return r.json()
        except Exception: return {}

    def ticker_price(self, symbol:str) -> float:
        r = requests.get(f"{self.base}/api/v3/ticker/price", params={"symbol": symbol}, timeout=TIMEOUT_S)
        j = r.json(); return float(j.get("price") or 0.0)

    def place_market(self, *, symbol:str, side:str,
                     quote_qty:float=None, base_qty:float=None, newClientOrderId:str=None):
        params = {"symbol": symbol.upper(), "side": side.upper(), "type": "MARKET"}
        if side.upper()=="BUY":
            if quote_qty is None: return None, {"error":"BUY requires quote_qty"}
            params["quoteOrderQty"] = f"{float(quote_qty):.8f}"
        else:
            if base_qty is None: return None, {"error":"SELL requires base_qty"}
            params["quantity"] = f"{float(base_qty):.8f}"
        if newClientOrderId: params["newClientOrderId"] = str(newClientOrderId)
        r = self._req("POST", "/api/v3/order", params)
        try: data = r.json()
        except Exception: data = {"raw": r.text}
        return r, data

def _symbol_norm(venue_symbol: str) -> str:
    # Accept BTC/USDT, BTCUSDT, BTC-USDT → BTCUSDT
    s = venue_symbol.replace("-", "").replace("/", "").upper()
    # Guard against accidental USDC on BinanceUS for BTC
    if s.endswith("USDC"): s = s.replace("USDC", "USDT")
    return s

def _floor_to_step(qty: float, step: float) -> float:
    if step <= 0: return qty
    return (int(qty / step) * step)

def execute_market_order(*, venue_symbol: str, side: str,
                         amount_quote: float = 0.0, amount_base: float = 0.0,
                         client_id: str = "", edge_mode: str = "dryrun",
                         edge_hold: bool = False, **_):
    symbol = _symbol_norm(venue_symbol)  # e.g., BTCUSDT
    if edge_hold:
        return {"status":"held","message":"EDGE_HOLD enabled","fills":[],"venue":"BINANCEUS","symbol":symbol,"side":side}

    # DRYRUN
    if edge_mode != "live":
        px = 60000.0
        qty = round((float(amount_quote or 0)/px) if side.upper()=="BUY" else float(amount_base or 0), 8)
        return {"status":"ok","txid":f"SIM-BUS-{int(time.time()*1000)}","fills":[{"qty":qty,"price":px}],
                "venue":"BINANCEUS","symbol":symbol,"side":side,"executed_qty":qty,"avg_price":px,
                "message":"binanceus dryrun simulated fill"}

    # LIVE requires keys
    if not (os.getenv("BINANCEUS_API_KEY") and os.getenv("BINANCEUS_API_SECRET")):
        return {"status":"error","message":"Missing BINANCEUS_API_KEY/SECRET","fills":[],
                "venue":"BINANCEUS","symbol":symbol,"side":side}

    bus = BinanceUS()

    # Fetch filters for lot size & min notional
    info = bus.exchange_info(symbol)
    try:
        sym = (info.get("symbols") or [])[0]
        filters = {f["filterType"]: f for f in sym.get("filters", [])}
        lot = filters.get("LOT_SIZE", {})
        step = float(lot.get("stepSize", "0.00001"))
        min_qty = float(lot.get("minQty", "0.00001"))
        min_notional = float(filters.get("NOTIONAL", {}).get("minNotional", "0"))
    except Exception:
        step, min_qty, min_notional = 1e-5, 1e-5, 0.0

    # Free balance clamp for SELL
    if side.upper() == "SELL":
        acct = bus.account()
        free_btc = 0.0
        for b in acct.get("balances", []):
            if (b.get("asset") or "").upper() == "BTC":
                try: free_btc = float(b.get("free") or 0.0)
                except Exception: free_btc = 0.0
        qty_req = float(amount_base or 0.0)
        qty = min(max(0.0, qty_req), max(0.0, free_btc - 1e-8))
        qty = _floor_to_step(qty, step)
        if qty < min_qty:
            return {"status":"error","message":f"Insufficient free balance after clamp (qty {qty} < minQty {min_qty})",
                    "fills":[], "venue":"BINANCEUS","symbol":symbol,"side":side}
        resp, data = bus.place_market(symbol=symbol, side="SELL", base_qty=qty, newClientOrderId=str(client_id))
    else:
        # BUY: optional min-notional hint
        q_spend = float(amount_quote or 0.0)
        if q_spend <= 0:
            return {"status":"error","message":"BUY requires amount_quote > 0","fills":[],
                    "venue":"BINANCEUS","symbol":symbol,"side":side}
        if min_notional and q_spend < min_notional:
            return {"status":"error","message":f"min notional {min_notional} not met (amount={q_spend})","fills":[],
                    "venue":"BINANCEUS","symbol":symbol,"side":side}
        resp, data = bus.place_market(symbol=symbol, side="BUY", quote_qty=q_spend, newClientOrderId=str(client_id))

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
