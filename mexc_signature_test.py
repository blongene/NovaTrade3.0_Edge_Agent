import hmac
import hashlib
import time
import requests
import os
from urllib.parse import urlencode
from dotenv import load_dotenv

load_dotenv()

BASE_URL = "https://api.mexc.com"
API_KEY = os.getenv("MEXC_API_KEY")
API_SECRET = os.getenv("MEXC_API_SECRET")

def sign(params, secret):
    query_string = urlencode(params)
    return hmac.new(secret.encode(), query_string.encode(), hashlib.sha256).hexdigest()

timestamp = int(time.time() * 1000)
params = {
    "timestamp": timestamp,
    "recvWindow": 5000
}
params["signature"] = sign(params, API_SECRET)

headers = {
    "X-MEXC-APIKEY": API_KEY
}

response = requests.get(f"{BASE_URL}/api/v3/account", headers=headers, params=params)

print(f"🔗 URL: {response.url}")
print(f"📦 Response: {response.status_code} | {response.text}")
