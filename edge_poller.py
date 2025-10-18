# edge_poller.py
import os, time, json, hmac, hashlib, requests

CLOUD_BASE = os.getenv("CLOUD_BASE_URL","http://localhost:10000")
EDGE_SECRET = os.getenv("EDGE_SECRET","")
EDGE_MODE   = os.getenv("EDGE_MODE","dryrun")  # 'dryrun' or 'live'
POLL_SECS   = int(os.getenv("EDGE_POLL_SECS","10"))

def _exec_trade(cmd):
    # TODO: call your existing exchange executors (MEXC/Kraken/Coinbase)
    # For now: simulate success and return a receipt
    p = cmd["payload"] ; sym = p["symbol"] ; side = p["side"] ; amt = p["amount_usd"]
    return {"ok": True, "symbol": sym, "side": side, "amount_usd": amt, "mode": EDGE_MODE, "ts": int(time.time())}

def run_poller():
    s = requests.Session()
    while True:
        try:
            r = s.post(f"{CLOUD_BASE}/api/commands/pull", json={"limit": 10}, timeout=15)
            cmds = (r.json().get("commands") or []) if r.ok else []
            for c in cmds:
                rec = _exec_trade(c)
                s.post(f"{CLOUD_BASE}/api/commands/ack",
                       json={"id": c["id"], "status": "DONE" if rec.get("ok") else "ERROR", "receipt": rec},
                       timeout=15)
        except Exception as e:
            print(f"[edge_poller] error: {e}")
        time.sleep(POLL_SECS)

if __name__ == "__main__":
    run_poller()
