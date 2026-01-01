# edge_bus_client.py — Signed client for Bus command endpoints
#
# Patch: tolerate multiple base-url env var names (BASE_URL, BUS_BASE_URL, CLOUD_BASE_URL, PUBLIC_BASE_URL)
# and keep behavior identical otherwise.

import os, json, hmac, hashlib, requests, time, random

_last_pull_log_ts = 0.0  # rate-limit noisy transient errors

# --------------------------------------------------------------------
# Config (all overridable via env)
# --------------------------------------------------------------------

def _pick_env(*keys: str) -> str:
    for k in keys:
        v = (os.getenv(k) or "").strip()
        if v:
            return v
    return ""


def _base_url() -> str:
    # Prefer explicit BASE_URL, but allow the newer envs you already use.
    base = _pick_env("BASE_URL", "BUS_BASE_URL", "CLOUD_BASE_URL", "PUBLIC_BASE_URL")
    if not (base.startswith("http://") or base.startswith("https://")):
        raise RuntimeError(
            f"Bus base URL missing/invalid. Set BASE_URL (or BUS_BASE_URL/CLOUD_BASE_URL/PUBLIC_BASE_URL). Got: {base!r}"
        )
    return base.rstrip("/")


BASE_URL = _base_url()                                  # validated
EDGE_SECRET = (os.environ.get("EDGE_SECRET") or os.environ.get("OUTBOX_SECRET") or "").strip()  # required for HMAC
if not EDGE_SECRET:
    raise RuntimeError("EDGE_SECRET is not set (must match Bus OUTBOX_SECRET)")

# timeouts/backoff
TIMEOUT  = float(os.environ.get("EDGE_HTTP_TIMEOUT_SECS", "12"))   # seconds (connect/read)
RETRIES  = int(os.environ.get("EDGE_HTTP_RETRIES", "4"))           # total attempts
BACKOFF0 = float(os.environ.get("EDGE_HTTP_BACKOFF_SECS", "0.6"))  # base backoff

# --------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------

def _canonical_json(d: dict) -> str:
    # Match Bus HMAC expectations: stable keys, compact separators
    return json.dumps(d, separators=(",", ":"), sort_keys=True)


def _sign_raw(raw: str) -> str:
    return hmac.new(EDGE_SECRET.encode("utf-8"), raw.encode("utf-8"), hashlib.sha256).hexdigest()


def _retryable_status(code: int) -> bool:
    # retry on 5xx + 429
    return code == 429 or (500 <= code < 600)


def _short_html(text: str, limit: int = 160) -> str:
    t = (text or "").strip()
    if t.startswith("<!DOCTYPE") or t.startswith("<html"):
        return "HTML error page (e.g., 502)"
    if len(t) > limit:
        return t[:limit] + "…"
    return t


# --------------------------------------------------------------------
# Core signed POST with retries
# --------------------------------------------------------------------

def post_signed(path: str, body: dict, timeout: float | tuple | None = None) -> requests.Response:
    """Signed POST with automatic retries on 5xx/429 and connection issues."""
    url = f"{BASE_URL}{path}"
    raw = _canonical_json(body)
    sig = _sign_raw(raw)

    headers = {
        "Content-Type": "application/json",
        "X-Nova-Signature": sig,
    }
    if timeout is None:
        timeout = (TIMEOUT, TIMEOUT)  # (connect, read)

    last_exc: Exception | None = None

    for attempt in range(1, RETRIES + 1):
        try:
            r = requests.post(url, data=raw, headers=headers, timeout=timeout)
            if _retryable_status(r.status_code):
                last_exc = requests.HTTPError(
                    f"{r.status_code} {_short_html(r.text)}", response=r
                )
            else:
                r.raise_for_status()
                return r

        except (requests.Timeout, requests.ConnectionError) as e:
            last_exc = e

        except requests.HTTPError as e:
            resp = getattr(e, "response", None)
            code = getattr(resp, "status_code", None)
            if code is None or not _retryable_status(code):
                raise
            last_exc = e

        # jittered exponential backoff
        sleep_s = BACKOFF0 * (2 ** (attempt - 1))
        sleep_s += random.uniform(0, 0.25)
        time.sleep(min(sleep_s, 6.0))

    if last_exc:
        raise last_exc
    raise RuntimeError("post_signed failed without exception")


# --------------------------------------------------------------------
# Convenience
# --------------------------------------------------------------------

def pull(agent_id: str, limit: int = 1) -> dict:
    """Lease up to `limit` commands for this agent."""
    global _last_pull_log_ts

    body = {"agent_id": agent_id, "limit": int(limit), "ts": int(time.time())}
    try:
        r = post_signed("/api/commands/pull", body)
        return r.json()
    except Exception as e:
        msg = getattr(e, "args", [str(e)])[0]
        code = getattr(getattr(e, "response", None), "status_code", None)
        now = time.time()

        transient = (code in (502, 503, 504)) or ("HTML error page" in str(msg)) or ("<!DOCTYPE" in str(msg))
        if transient:
            if now - _last_pull_log_ts > 60:
                print(f"[edge] WARN pull transient {code or ''}: {msg}")
                _last_pull_log_ts = now
        else:
            print(f"[edge] ERROR pull error: {msg}")

        return {"ok": False, "commands": [], "error": msg}


def ack(agent_id: str, cmd_id: int | str, ok: bool, receipt: dict) -> dict:
    """Acknowledge a leased command with a normalized receipt."""
    body = {
        "agent_id": agent_id,
        "cmd_id": cmd_id,
        "ok": bool(ok),
        "receipt": receipt or {},
        "ts": int(time.time()),
    }
    r = post_signed("/api/commands/ack", body)
    r.raise_for_status()
    return r.json()
