import os, json, hmac, hashlib, requests, time, random

BASE_URL = os.environ.get("BASE_URL", "").rstrip("/")
SECRET   = os.environ.get("EDGE_SECRET", "")
TIMEOUT  = float(os.environ.get("EDGE_HTTP_TIMEOUT_SECS", "12"))  # connect/read
RETRIES  = int(os.environ.get("EDGE_HTTP_RETRIES", "4"))          # attempts total
BACKOFF0 = float(os.environ.get("EDGE_HTTP_BACKOFF_SECS", "0.6")) # base backoff

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

def _sign(raw: str) -> str:
    return hmac.new(SECRET.encode("utf-8"), raw.encode("utf-8"), hashlib.sha256).hexdigest()

def post_signed(path: str, body: dict, timeout: float | tuple = None) -> requests.Response:
    """Signed POST with automatic retries on 5xx and connection issues."""
    url = f"{BASE_URL}{path}"
    raw = json.dumps(body, separators=(",", ":"), sort_keys=True)
    sig = _sign(raw)
    headers = {
        "Content-Type": "application/json",
        "X-Nova-Signature": sig,
    }
    if timeout is None:
        timeout = (TIMEOUT, TIMEOUT)  # (connect, read)

    last_exc = None
    for attempt in range(1, RETRIES + 1):
        try:
            r = requests.post(url, data=raw, headers=headers, timeout=timeout)
            # Fast-fail on auth problems; retry only 5xx and gateway errors
            if 500 <= r.status_code < 600 or r.status_code in (429,):
                # transient – retry
                pass
            else:
                r.raise_for_status()
                return r
        except (requests.Timeout, requests.ConnectionError) as e:
            last_exc = e
        except requests.HTTPError as e:
            # if not a retryable code, bubble immediately
            if not (500 <= e.response.status_code < 600 or e.response.status_code == 429):
                raise
            last_exc = e

        # Backoff with jitter
        sleep_s = BACKOFF0 * (2 ** (attempt - 1))
        sleep_s += random.uniform(0, 0.25)
        time.sleep(min(sleep_s, 6.0))

    # all attempts failed
    if last_exc:
        raise last_exc
    raise RuntimeError("post_signed failed without exception")

def pull(agent_id: str, limit: int = 1) -> dict:
    body = {"agent_id": agent_id, "limit": int(limit), "ts": int(time.time())}
    try:
        r = post_signed("/api/commands/pull", body)
        return r.json()
    except Exception as e:
        # Don’t crash the loop – surface a clean, single-line message
        print(f"[edge] ERROR pull error: {e}")
        return {"ok": False, "commands": [], "error": str(e)}

def ack(agent_id: str, cmd_id: int | str, ok: bool, receipt: dict) -> dict:
    body = {"agent_id": agent_id, "cmd_id": cmd_id, "ok": ok, "receipt": receipt, "ts": int(time.time())}
    r = post_signed("/api/commands/ack", body); r.raise_for_status(); return r.json()

def _base_url() -> str:
    base = os.getenv("BASE_URL", "").strip()
    if not (base.startswith("http://") or base.startswith("https://")):
        raise RuntimeError(f"BASE_URL missing or invalid: {base!r}")
    return base.rstrip("/")

