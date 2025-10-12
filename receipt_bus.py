# receipt_bus.py — Edge → Cloud receipt ACK with HMAC
import os, time, hmac, hashlib, json, requests

AGENT_ID = os.getenv("AGENT_ID") or os.getenv("EDGE_AGENT_ID")
if not AGENT_ID:
    raise RuntimeError(
        "AGENT_ID is required for receipt_bus. Set AGENT_ID to match the edge "
        "agent (e.g., edge-cb-1, edge-bus-1) before running."
    )

CLOUD_BASE_URL = os.getenv("CLOUD_BASE_URL", "http://localhost:10000")  # override in env
EDGE_SECRET    = os.getenv("EDGE_SECRET", "")
TIMEOUT        = 15

def _sign(payload: dict) -> str:
    body = json.dumps(payload, separators=(",", ":"), ensure_ascii=False)
    return hmac.new(EDGE_SECRET.encode(), body.encode(), hashlib.sha256).hexdigest()

def post_receipt(cmd: dict, normalized: dict, raw: dict | None = None) -> dict:
    if not CLOUD_BASE_URL:
        return {"ok": False, "error": "CLOUD_BASE_URL missing"}
    if not EDGE_SECRET:
        return {"ok": False, "error": "EDGE_SECRET missing"}

    payload = {
        "agent_id": AGENT_ID,
        "cmd_id": cmd.get("id"),
        "ts": int(time.time()*1000),
        "normalized": normalized,
        "raw": raw or {},
    }
    sig = _sign(payload)
    headers = {"Content-Type": "application/json", "X-Nova-Signature": sig}
    r = requests.post(f"{CLOUD_BASE_URL}/api/receipts/ack",
                      data=json.dumps(payload, ensure_ascii=False),
                      headers=headers, timeout=TIMEOUT)
    try:
        r.raise_for_status()
        return {"ok": True, "cloud_rcpt": r.json()}
    except Exception:
        return {"ok": False, "status": r.status_code, "body": r.text}
