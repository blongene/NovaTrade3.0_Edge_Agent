# rebuy_driver.py — Edge Command Executor
# Pulls commands, Execs (Stub), Acks with Canonical HMAC.

import os, time, requests, json
import hmac_utils

# Config
BUS_BASE = (os.getenv("BUS_BASE_URL") or "https://novatrade3-0.onrender.com").rstrip("/")
PULL_URL = f"{BUS_BASE}/api/commands/pull"
ACK_URL  = f"{BUS_BASE}/api/commands/ack"
SECRET   = os.getenv("OUTBOX_SECRET") or ""
AGENT_ID = os.getenv("AGENT_ID", "edge-primary")

def pull_commands():
    payload = {"agent_id": AGENT_ID, "limit": 5}
    # Sign Pull Request
    headers = hmac_utils.get_auth_headers(payload, SECRET)
    try:
        body = hmac_utils.canonical_bytes(payload)
        r = requests.post(PULL_URL, data=body, headers=headers, timeout=10)
        if r.ok:
            return r.json().get("commands", [])
        return []
    except Exception as e:
        print(f"[Driver] Pull Error: {e}")
        return []

def ack_command(cmd_id, status, receipt):
    payload = {
        "agent_id": AGENT_ID,
        "id": cmd_id,
        "status": status,
        "receipt": receipt
    }
    # Sign Ack Request
    headers = hmac_utils.get_auth_headers(payload, SECRET)
    try:
        body = hmac_utils.canonical_bytes(payload)
        requests.post(ACK_URL, data=body, headers=headers, timeout=10)
        print(f"[Driver] Ack Sent: {cmd_id} -> {status}")
    except Exception as e:
        print(f"[Driver] Ack Error: {e}")

def execute_stub(cmd):
    """Real execution logic goes here (CCXT)."""
    print(f"[Exec] {cmd}")
    return True, {"msg": "Executed stub"}

def run_loop():
    print(f"Starting Rebuy Driver | Agent: {AGENT_ID}")
    while True:
        cmds = pull_commands()
        for c in cmds:
            cid = c.get("id")
            payload = c.get("payload")
            
            # Execute
            ok, receipt = execute_stub(payload)
            status = "ok" if ok else "error"
            
            # Ack
            ack_command(cid, status, receipt)
            
        time.sleep(5)

if __name__ == "__main__":
    run_loop()
