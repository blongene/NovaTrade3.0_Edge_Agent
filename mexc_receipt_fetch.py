# mexc_receipt_fetch.py
import os, time, hmac, hashlib, urllib.parse, requests, json
from dotenv import load_dotenv; load_dotenv()

BASE  = (os.getenv("MEXC_BASE_URL") or "https://api.mexc.com").rstrip("/")
KEY   = os.getenv("MEXC_KEY") or os.getenv("MEXC_API_KEY") or ""
SECRET= os.getenv("MEXC_SECRET") or os.getenv("MEXC_API_SECRET") or ""

def _sign(params: dict) -> str:
    qs = urllib.parse.urlencode(params)
    return hmac.new(SECRET.encode(), qs.encode(), hashlib.sha256).hexdigest()

def get_order(symbol: str, order_id: str):
    params = {"symbol": symbol.replace("/","").upper(), "orderId": order_id, "timestamp": int(time.time()*1000), "recvWindow": 5000}
    sig = _sign(params)
    r = requests.get(f"{BASE}/api/v3/order", params={**params, "signature": sig}, headers={"X-MEXC-APIKEY": KEY}, timeout=15)
    return r.status_code, r.json() if r.headers.get("content-type","").startswith("application/json") else r.text

def get_trades(symbol: str, order_id: str):
    params = {"symbol": symbol.replace("/","").upper(), "orderId": order_id, "timestamp": int(time.time()*1000), "recvWindow": 5000}
    sig = _sign(params)
    r = requests.get(f"{BASE}/api/v3/myTrades", params={**params, "signature": sig}, headers={"X-MEXC-APIKEY": KEY}, timeout=15)
    return r.status_code, r.json() if r.headers.get("content-type","").startswith("application/json") else r.text

if __name__ == "__main__":
    # paste one of your orderIds here:
    order_ids = [
        "C02__592712390852882432028",
        "C02__592712395454066688028",
    ]
    for oid in order_ids:
        print("\nORDER", oid)
        sc, jo = get_order("MX/USDT", oid)
        print("order:", sc, json.dumps(jo, ensure_ascii=False))
        sc, jt = get_trades("MX/USDT", oid)
        print("trades:", sc, json.dumps(jt, ensure_ascii=False))
