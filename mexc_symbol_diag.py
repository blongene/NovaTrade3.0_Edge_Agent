import os, requests, sys
sym = (sys.argv[1] if len(sys.argv) > 1 else "BTCUSDT").upper()
BASE = (os.getenv("MEXC_BASE_URL") or "https://api.mexc.com").rstrip("/")
r = requests.get(f"{BASE}/api/v3/exchangeInfo", params={"symbol": sym}, timeout=15)
print("status:", r.status_code)
print(r.text)
