import os
import time
import hmac
import hashlib
import requests
from urllib.parse import urlencode
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("MEXC_API_KEY")
API_SECRET = os.getenv("MEXC_API_SECRET")
BASE_URL = "https://api.mexc.com"

def _sign(params):
    query_string = urlencode(params)
    signature = hmac.new(API_SECRET.encode(), query_string.encode(), hashlib.sha256).hexdigest()
    params['signature'] = signature
    return params

def get_balance(asset="USDT"):
    try:
        timestamp = int(time.time() * 1000)
        params = {
            "timestamp": timestamp
        }
        signed = _sign(params)
        headers = {
            "Content-Type": "application/json",
            "ApiKey": API_KEY
        }
        url = f"{BASE_URL}/api/v3/account"
        response = requests.get(url, headers=headers, params=signed)
        response.raise_for_status()
        balances = response.json().get("balances", [])
        for bal in balances:
            if bal["asset"] == asset:
                return float(bal["free"])
        return 0.0
    except Exception as e:
        print("❌ Error:", e)
        return None

usdt_balance = get_balance("USDT")
print(f"💰 Your MEXC USDT Balance: {usdt_balance}")
