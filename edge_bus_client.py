# edge_bus_client.py — Signed client for Bus command endpoints (bulletproof)
#
# Key fix: Bus may expect signature in X-OUTBOX-SIGN (Phase 24/25),
# while some older/other code used X-Nova-Signature.
# We send BOTH headers to be tolerant.
#
# Env:
#   BUS_BASE_URL or CLOUD_BASE_URL or BASE_URL or PUBLIC_BASE_URL
#   EDGE_SECRET (preferred) OR OUTBOX_SECRET (fallback)
#
# Optional:
#   EDGE_HTTP_TIMEOUT_SECS, EDGE_HTTP_RETRIES, EDGE_HTTP_BACKOFF_SECS

from __future__ import annotations

import os
import json
import hmac
import hashlib
import time
import random
from typing import Any, Dict

import requests

_last_pull_log_ts = 0.0  # rate-limit noisy transient errors


def _pick_env(*keys: str) -> str:
    for k in keys:
        v = (os.getenv(k) or "").strip()
        if v:
            return v
    return ""


def _base_url() -> str:
    base = _pick_env("BUS_BASE_URL", "CLOUD_BASE_URL", "BASE_URL", "PUBLIC_BASE_URL")
    if not (base.startswith("http://") or base.startswith("https://")):
        raise RuntimeError(
            f"Bus base URL missing/invalid. Set BUS_BASE_URL (or CLOUD_BASE_URL/BASE_URL/PUBLIC_BASE_URL). Got: {base!r}"
        )
    return base.rstrip("/")


BASE_URL = _base_url()

EDGE_SECRET = (_pick_env("EDGE_SECRET", "OUTBOX_SECRET")).strip()
if not EDGE_SECRET:
    raise RuntimeError("EDGE_SECRET is not set (must match Bus OUTBOX_SECRET)")


# timeouts/backoff
TIMEOUT = float(os.environ.get("EDGE_HTTP_TIMEOUT_SECS", "12"))
RETRIES = int(os.environ.get("EDGE_HTTP_RETRIES", "4"))
BACKOFF0 = float(os.environ.get("EDGE_HTTP_BACKOFF_SECS", "0.6"))


def _canonical_json(d: Dict[str, Any]) -> str:
    # Must match Bus HMAC expectations: stable keys, compact separators
    return json.dumps(d, separators=(",", ":"), sort_keys=True)


def _sign_raw(raw: str) -> str:
    return hmac.new(EDGE_SECRET.encode("utf-8"), raw.encode("utf-8"), hashlib.sha256).hexdigest()


def _retryable_status(code: int) -> bool:
    return code == 429 or (500 <= code < 600)


def _short_html(text: str, limit: int = 160) -> str:
    t = (text or "").strip()
    if t.startswith("<!DOCTYPE") or t.startswith("<html"):
        return "HTML error page (e.g., 502)"
    return (t[:limit] + "…") if len(t) > limit else t


def post_signed(path: str, body: Dict[str, Any], timeout: float | tuple | None = None) -> requests.Response:
    """Signed POST with automatic retries on 5xx/429 and connection issues."""
    url = f"{BASE_URL}{path}"
    raw = _canonical_json(body)
    sig = _sign_raw(raw)

    # Bulletproof: send both common signature headers
    headers = {
        "Content-Type": "application/json",
        "X-OUTBOX-SIGN": sig,
        "X-Nova-Signature": sig,
    }

    if timeout is None:
        timeout = (TIMEOUT, TIMEOUT)

    last_exc: Exception | None = None

    for attempt in range(1, RETRIES + 1):
        try:
            r = requests.post(url, data=raw, headers=headers, timeout=timeout)

            # IMPORTANT: 401/403 are NOT retryable; surface immediately
            if r.status_code in (401, 403):
                r.raise_for_status()

            if _retryable_status(r.status_code):
                last_exc = requests.HTTPError(f"{r.status_code} {_short_html(r.text)}", response=r)
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
        sleep_s = BACKOFF0 * (2 ** (attempt - 1)) + random.uniform(0, 0.25)
        time.sleep(min(sleep_s, 6.0))

    if last_exc:
        raise last_exc
    raise RuntimeError("post_signed failed without exception")


def pull(agent_id: str, limit: int = 1) -> Dict[str, Any]:
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

        transient = (code in (502, 503, 504, 429)) or ("HTML error page" in str(msg)) or ("<!DOCTYPE" in str(msg))
        if transient:
            if now - _last_pull_log_ts > 60:
                print(f"[edge] WARN pull transient {code or ''}: {msg}")
                _last_pull_log_ts = now
        else:
            print(f"[edge] ERROR pull error: {code or ''} {msg}")

        return {"ok": False, "commands": [], "error": msg}


def ack(agent_id: str, cmd_id: int | str, ok: bool, receipt: Dict[str, Any]) -> Dict[str, Any]:
    """Acknowledge a leased command with a normalized receipt."""
    body = {
        "agent_id": agent_id,
        "cmd_id": cmd_id,
        "ok": bool(ok),
        "receipt": receipt or {},
        "ts": int(time.time()),
    }
    r = post_signed("/api/commands/ack", body)
    return r.json()
