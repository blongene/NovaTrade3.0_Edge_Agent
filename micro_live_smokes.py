# micro_live_smokes.py
import os, json
from dotenv import load_dotenv; load_dotenv()
os.environ["EDGE_MODE"] = "live"  # force live for this one script

from broker_router import execute

# 1) MEXC $5 BUY BTC/USDT (should fill)
mexc_cmd = {
  "id":"smoke_mexc_5",
  "type":"order.place",
  "payload":{"venue":"MEXC","symbol":"MX/USDT","side":"BUY","quote_amount":5}
}

print("SEND →", mexc_cmd["payload"])
print("RCPT →", json.dumps(execute(mexc_cmd), ensure_ascii=False))

# 2) Kraken $5 BUY BTC/USDT -> XBTUSDT (run only if you have USDT on Kraken)
# micro_live_smokes.py
kraken_symbol = "BTC/USDT"  # or "BTC/USD" if you funded USD
kraken_cmd = {
    "id": "smoke_kraken_15",
    "type": "order.place",
    "payload": {"venue":"KRAKEN","symbol":kraken_symbol,"side":"BUY","quote_amount":15}
}

for cmd in (kraken_cmd,):  # add kraken_cmd here after you fund Kraken with USDT/USD
    print("SEND →", cmd["payload"])
    r = execute(cmd)
    print("RCPT →", json.dumps(r, ensure_ascii=False))
