# test_router_local.py
import os, json
os.environ.setdefault("EDGE_MODE", "dryrun")
from dotenv import load_dotenv; load_dotenv()

from broker_router import execute

tests = [
  {"id":"t1","type":"order.place","payload":{"venue":"MEXC","symbol":"BTC/USDT","side":"BUY","quote_amount":10}},
  {"id":"t2","type":"order.place","payload":{"venue":"KRAKEN","symbol":"BTC/USDT","side":"BUY","quote_amount":10}},
  {"id":"t3","type":"order.place","payload":{"venue":"UNKNOWN","symbol":"BTC/USDT","side":"BUY","quote_amount":10}},
]
for t in tests:
    print(t["id"], "→", json.dumps(execute(t), separators=(',',':')))
