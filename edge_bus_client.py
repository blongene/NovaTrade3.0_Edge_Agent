import os, json, hmac, hashlib, requests, time

def _base_url() -> str:
    base = os.getenv("BASE_URL","").strip()
    if not (base.startswith("http://") or base.startswith("https://")):
        raise RuntimeError(f"BASE_URL missing or invalid: {base!r}")
    return base.rstrip("/")

EDGE_SECRET = os.getenv("EDGE_SECRET","")

def _raw(d: dict) -> str:
    return json.dumps(d, separators=(",",":"))

def _sign_raw(d: dict) -> str:
    raw = _raw(d)
    return hmac.new(EDGE_SECRET.encode(), raw.encode(), hashlib.sha256).hexdigest()
def post_signed(path: str, body: dict, timeout: int = 15) -> requests.Response:
    base = _base_url()
    raw  = _raw(body)
    sig  = _sign_raw(body)
    headers = {"Content-Type":"application/json", "X-Nova-Signature": sig}

    if os.getenv("EDGE_DEBUG_HMAC","").lower() in ("1","true","yes"):
        try:
            dbg = requests.post(f"{base}/api/debug/hmac_check", data=raw,
                                headers={"Content-Type":"application/json"}, timeout=timeout)
            print("[edge:hmac_dbg]", {"raw": raw, "sig": sig, "check": dbg.text})
        except Exception as e:
            print("[edge:hmac_dbg] failed:", e)

    return requests.post(f"{base}{path}", data=raw, headers=headers, timeout=timeout)
    
def pull(agent_id: str, limit: int = 3) -> dict:
    body = {"agent_id": agent_id, "limit": limit, "ts": int(time.time())}
    r = post_signed("/api/commands/pull", body); r.raise_for_status(); return r.json()

def ack(agent_id: str, cmd_id: int | str, ok: bool, receipt: dict) -> dict:
    body = {"agent_id": agent_id, "cmd_id": cmd_id, "ok": ok, "receipt": receipt, "ts": int(time.time())}
    r = post_signed("/api/commands/ack", body); r.raise_for_status(); return r.json()

def _base_url() -> str:
    base = os.getenv("BASE_URL", "").strip()
    if not (base.startswith("http://") or base.startswith("https://")):
        raise RuntimeError(f"BASE_URL missing or invalid: {base!r}")
    return base.rstrip("/")

