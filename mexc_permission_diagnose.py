import os
import time
import hmac
import hashlib
import requests
from dotenv import load_dotenv

# Load from .env if applicable
load_dotenv()

API_KEY = os.getenv("MEXC_API_KEY")
API_SECRET = os.getenv("MEXC_API_SECRET")
BASE_URL = "https://api.mexc.com"

# Fetch all supported symbols
def get_supported_symbols():
    try:
        r = requests.get(f"{BASE_URL}/api/v3/exchangeInfo")
        return [s["symbol"] for s in r.json()["symbols"]]
    except Exception as e:
        print(f"❌ Failed to fetch exchange info: {e}")
        return []

# Sign request parameters
def sign(params):
    query = '&'.join([f"{k}={v}" for k, v in sorted(params.items())])
    sig = hmac.new(API_SECRET.encode(), query.encode(), hashlib.sha256).hexdigest()
    return f"{query}&signature={sig}"

# Test an individual endpoint
def test_endpoint(name, method, path, params={}):
    params["timestamp"] = int(time.time() * 1000)
    query = sign(params)
    headers = {"X-MEXC-APIKEY": API_KEY}
    url = f"{BASE_URL}{path}?{query}"
    r = requests.request(method, url, headers=headers)
    try:
        json = r.json()
    except:
        json = r.text
    status = "✅" if r.status_code == 200 else "❌"
    print(f"{status} {name}: {r.status_code} - {json}")
    return r.status_code, json

# Perform a safe simulated trade
def simulate_trade(symbol="DOGEUSDT", usdt_threshold=1):
    # Step 1: Fetch price
    try:
        price_data = requests.get(f"{BASE_URL}/api/v3/ticker/price", params={"symbol": symbol}).json()
        price = float(price_data["price"])
    except Exception as e:
        print(f"❌ Price fetch failed: {e}")
        return

    quantity = 1
    usdt_equiv = quantity * price

    # Step 2: Check value threshold
    if usdt_equiv < usdt_threshold:
        print(f"❌ Skipping trade test — value too low ({usdt_equiv:.4f} USDT < {usdt_threshold} USDT)")
    else:
        test_endpoint("Trade (Simulated)", "POST", "/api/v3/order", {
            "symbol": symbol,
            "side": "BUY",
            "type": "MARKET",
            "quantity": quantity
        })

# === Execution ===

if __name__ == "__main__":
    print("\n🔍 Diagnosing MEXC API Permissions...\n")

    # Show supported trading pairs (first 10 for sanity check)
    symbols = get_supported_symbols()
    print("📄 Top symbols:", symbols[:10])

    # Run diagnostics
    test_endpoint("Balance Fetch", "GET", "/api/v3/account")
    test_endpoint("Order Book Access", "GET", "/api/v3/depth", {"symbol": "DOGEUSDT"})

    # Safe trade test
    simulate_trade("DOGEUSDT")
