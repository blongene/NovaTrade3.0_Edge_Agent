# telemetry.py — Edge Balance Reporter
# Pushes balances to Bus using Canonical HMAC.

import os, time, json, requests
from typing import Dict
import hmac_utils  # Uses our new shared signer

# Config
BUS_BASE = (os.getenv("BUS_BASE_URL") or "https://novatrade3-0.onrender.com").rstrip("/")
PUSH_URL = f"{BUS_BASE}/api/telemetry/push"
SECRET   = os.getenv("TELEMETRY_SECRET") or os.getenv("OUTBOX_SECRET") or ""
AGENT_ID = os.getenv("AGENT_ID", "edge-primary")
POLL_SEC = int(os.getenv("EDGE_POLL_SECS", "60"))

def get_mock_balances() -> Dict[str, Dict[str, float]]:
    """
    Replace this with your actual CCXT/Exchange polling logic.
    Returns: { "BINANCEUS": {"BTC": 0.1, "USDT": 500}, ... }
    """
    # For now, we return a safe structure. 
    # If you have existing logic, PASTE IT HERE.
    return {} 

def push_telemetry():
    balances = get_mock_balances()
    
    payload = {
        "agent_id": AGENT_ID,
        "ts": int(time.time()),
        "balances": balances
    }
    
    # SIGNING FIX: Use canonical signer
    headers = hmac_utils.get_auth_headers(payload, SECRET)
    
    try:
        # Send RAW canonical bytes to ensure no re-serialization drift
        # requests.post(json=...) might re-serialize with spaces
        body_bytes = hmac_utils.canonical_bytes(payload)
        
        r = requests.post(PUSH_URL, data=body_bytes, headers=headers, timeout=10)
        if r.ok:
            print(f"[Telemetry] Push OK: {r.status_code}")
        else:
            print(f"[Telemetry] Push Fail: {r.status_code} {r.text}")
    except Exception as e:
        print(f"[Telemetry] Error: {e}")

def run_loop():
    print(f"Starting Telemetry Loop | Agent: {AGENT_ID} | Bus: {BUS_BASE}")
    while True:
        push_telemetry()
        time.sleep(POLL_SEC)

if __name__ == "__main__":
    run_loop()
