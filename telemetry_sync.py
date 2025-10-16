# telemetry_sync.py — Edge telemetry/heartbeat push to Bus with HMAC
import os, json, time, hmac, hashlib, requests
from typing import Dict, Any
import telemetry_db

BUS_PUSH_URL   = os.getenv("TELEMETRY_PUSH_URL", "").rstrip("/")
HB_URL         = os.getenv("HEARTBEAT_URL", "").rstrip("/")
AGENT_ID       = os.getenv("EDGE_AGENT_ID", "edge-cb-1")
TELEM_SECRET   = os.getenv("TELEMETRY_SECRET", "")  # separate from OUTBOX_SECRET
TIMEOUT        = int(os.getenv("TELEMETRY_TIMEOUT_S", "12"))

def _hmac_hex(secret: str, raw: bytes) -> str:
    return hmac.new(secret.encode("utf-8"), raw, hashlib.sha256).hexdigest()

def push_telemetry():
    if not BUS_PUSH_URL or not TELEM_SECRET:
        return {"ok": False, "error": "missing TELEMETRY_PUSH_URL or TELEMETRY_SECRET"}
    payload = {
        "agent": AGENT_ID,
        "ts": int(time.time()),
        "aggregates": telemetry_db.aggregates(24 * 3600),
    }
    raw = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
    sig = _hmac_hex(TELEM_SECRET, raw)
    r = requests.post(
        BUS_PUSH_URL + "/api/telemetry/push",
        data=raw,
        headers={"Content-Type": "application/json", "X-Signature": sig},
        timeout=TIMEOUT,
    )
    ok = (r.status_code == 200)
    return {"ok": ok, "status": r.status_code, "body": r.text if not ok else r.json()}

def send_heartbeat(latency_ms: int = 0):
    if not HB_URL or not TELEM_SECRET:
        return {"ok": False, "error": "missing HEARTBEAT_URL or TELEMETRY_SECRET"}
    payload = {"agent": AGENT_ID, "ts": int(time.time()), "latency_ms": int(latency_ms)}
    raw = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
    sig = _hmac_hex(TELEM_SECRET, raw)
    r = requests.post(
        HB_URL + "/api/heartbeat",
        data=raw,
        headers={"Content-Type": "application/json", "X-Signature": sig},
        timeout=TIMEOUT,
    )
    ok = (r.status_code == 200)
    return {"ok": ok, "status": r.status_code, "body": r.text if not ok else r.json()}
