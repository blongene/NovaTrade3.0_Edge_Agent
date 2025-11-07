# edge_bus_client.py — canonical Bus client for Edge (raw-bytes HMAC)
import os, json, hmac, hashlib, requests, time

BASE_URL = os.getenv("BASE_URL", "")
EDGE_SECRET = os.getenv("EDGE_SECRET", "")

def _raw(d: dict) -> str:
    return json.dumps(d, separators=(",", ":"))

def _sign_raw(d: dict) -> str:
    if not EDGE_SECRET:
        raise RuntimeError("EDGE_SECRET missing")
    raw = _raw(d)
    return hmac.new(EDGE_SECRET.encode(), raw.encode(), hashlib.sha256).hexdigest()

def post_signed(path: str, body: dict, timeout: int = 15) -> requests.Response:
    raw = _raw(body)
    sig = _sign_raw(body)
    return requests.post(
        f"{BASE_URL}{path}",
        data=raw,
        headers={"Content-Type": "application/json", "X-Nova-Signature": sig},
        timeout=timeout,
    )

def pull(agent_id: str, limit: int = 3) -> dict:
    body = {"agent_id": agent_id, "limit": limit, "ts": int(time.time())}
    r = post_signed("/api/commands/pull", body)
    r.raise_for_status()
    return r.json()

def ack(agent_id: str, cmd_id: int | str, ok: bool, receipt: dict) -> dict:
    body = {"agent_id": agent_id, "cmd_id": cmd_id, "ok": ok, "receipt": receipt, "ts": int(time.time())}
    r = post_signed("/api/commands/ack", body)
    r.raise_for_status()
    return r.json()
