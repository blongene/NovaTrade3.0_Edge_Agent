# edge_bus_client.py — Signed client for Bus command endpoints (Phase 25C auth-tolerant)
#
# Fixes:
# - Tolerates multiple base URL env var names (BASE_URL, BUS_BASE_URL, CLOUD_BASE_URL, PUBLIC_BASE_URL)
# - Tolerates multiple HMAC secrets (OUTBOX_SECRET, EDGE_SECRET, TELEMETRY_SECRET) — OUTBOX_SECRET first
# - Tolerates multiple signature header names used by Bus: X-OUTBOX-SIGN, X-Nova-Signature, X-NT-Sig
# - On 401, automatically retries with alternate header/secret combos
#
# Endpoints used:
#   POST /api/commands/pull
#   POST /api/commands/ack

from __future__ import annotations

import os
import json
import hmac
import hashlib
import time
import random
from typing import Dict, Any, List, Tuple, Optional

import requests

_last_pull_log_ts = 0.0  # rate-limit noisy transient errors


# --------------------------------------------------------------------
# Config helpers
# --------------------------------------------------------------------

def _pick_env(*keys: str) -> str:
    for k in keys:
        v = (os.getenv(k) or "").strip()
        if v:
            return v
    return ""


def _base_url() -> str:
    base = _pick_env("BASE_URL", "BUS_BASE_URL", "CLOUD_BASE_URL", "PUBLIC_BASE_URL")
    if not base:
        raise RuntimeError(
            "Bus base URL missing. Set BASE_URL (or BUS_BASE_URL/CLOUD_BASE_URL/PUBLIC_BASE_URL)."
        )
    if not (base.startswith("http://") or base.startswith("https://")):
        raise RuntimeError(f"Bus base URL invalid (must start with http/https). Got: {base!r}")
    return base.rstrip("/")


BASE_URL = _base_url()

# Timeouts/backoff
TIMEOUT = float(os.environ.get("EDGE_HTTP_TIMEOUT_SECS", "12"))   # seconds (connect/read)
RETRIES = int(os.environ.get("EDGE_HTTP_RETRIES", "4"))           # total attempts (for retryable errors)
BACKOFF0 = float(os.environ.get("EDGE_HTTP_BACKOFF_SECS", "0.6")) # base backoff


# --------------------------------------------------------------------
# Signing
# --------------------------------------------------------------------

def _canonical_json(d: dict) -> str:
    # Match Bus-side HMAC expectations: stable keys, compact separators
    return json.dumps(d, separators=(",", ":"), sort_keys=True)


def _sign_raw(secret: str, raw: str) -> str:
    return hmac.new(secret.encode("utf-8"), raw.encode("utf-8"), hashlib.sha256).hexdigest()


def _secret_candidates() -> List[str]:
    # Phase25C reality:
    # - /api/commands/* often uses OUTBOX_SECRET (cloud) but edge may store it as EDGE_SECRET
    # - Some setups reused TELEMETRY_SECRET (not preferred, but tolerating helps recovery)
    seen = set()
    out: List[str] = []
    for k in ("OUTBOX_SECRET", "EDGE_SECRET", "TELEMETRY_SECRET"):
        v = (os.getenv(k) or "").strip()
        if v and v not in seen:
            seen.add(v)
            out.append(v)
    return out


def _header_candidates() -> List[str]:
    # You’ve observed Bus verifying several headers across endpoints:
    # - X-OUTBOX-SIGN
    # - X-Nova-Signature
    # - X-NT-Sig
    return ["X-OUTBOX-SIGN", "X-Nova-Signature", "X-NT-Sig"]


def _retryable_status(code: int) -> bool:
    return code == 429 or (500 <= code < 600)


def _short_text(text: str, limit: int = 180) -> str:
    t = (text or "").strip()
    if t.startswith("<!DOCTYPE") or t.startswith("<html"):
        return "HTML error page (e.g., 502)"
    return (t[:limit] + "…") if len(t) > limit else t


# --------------------------------------------------------------------
# Core signed POST with retries + auth fallback
# --------------------------------------------------------------------

def post_signed(path: str, body: dict, timeout: Optional[float | Tuple[float, float]] = None) -> requests.Response:
    """
    Signed POST with:
      - automatic retries on 5xx/429 and connection issues
      - automatic auth fallback on 401 (tries alternate header/secret combos)
    """
    url = f"{BASE_URL}{path}"
    raw = _canonical_json(body)

    secrets = _secret_candidates()
    if not secrets:
        raise RuntimeError("No signing secret found. Set OUTBOX_SECRET or EDGE_SECRET on Edge.")

    header_names = _header_candidates()

    if timeout is None:
        timeout = (TIMEOUT, TIMEOUT)

    # Build auth combos: try OUTBOX_SECRET first, then EDGE_SECRET, etc; for each, try headers in order.
    combos: List[Tuple[str, str]] = []
    for sec in secrets:
        for hdr in header_names:
            combos.append((sec, hdr))

    last_exc: Optional[Exception] = None

    # First, try auth combos (fast) — only re-loop on 401.
    for sec, hdr in combos:
        sig = _sign_raw(sec, raw)
        headers = {"Content-Type": "application/json", hdr: sig}

        try:
            r = requests.post(url, data=raw, headers=headers, timeout=timeout)

            # If auth fails, try next combo immediately.
            if r.status_code == 401:
                last_exc = requests.HTTPError(f"401 {_short_text(r.text)}", response=r)
                continue

            # For retryable server/rate errors, fall through to retry loop below.
            if _retryable_status(r.status_code):
                last_exc = requests.HTTPError(f"{r.status_code} {_short_text(r.text)}", response=r)
                break

            r.raise_for_status()
            return r

        except (requests.Timeout, requests.ConnectionError) as e:
            last_exc = e
            break

        except requests.HTTPError as e:
            resp = getattr(e, "response", None)
            code = getattr(resp, "status_code", None)
            if code == 401:
                last_exc = e
                continue
            if code is None or not _retryable_status(code):
                raise
            last_exc = e
            break

    # If we got here, either we exhausted 401 combos OR hit retryable/network.
    # Now do a bounded retry loop (re-using the *first* auth combo) for transient issues.
    sec0, hdr0 = combos[0]
    sig0 = _sign_raw(sec0, raw)
    headers0 = {"Content-Type": "application/json", hdr0: sig0}

    for attempt in range(1, RETRIES + 1):
        try:
            r = requests.post(url, data=raw, headers=headers0, timeout=timeout)

            if r.status_code == 401:
                # If we’re *still* 401 here, surface it clearly (don’t waste time retrying).
                r.raise_for_status()

            if _retryable_status(r.status_code):
                last_exc = requests.HTTPError(f"{r.status_code} {_short_text(r.text)}", response=r)
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

        sleep_s = BACKOFF0 * (2 ** (attempt - 1)) + random.uniform(0, 0.25)
        time.sleep(min(sleep_s, 6.0))

    if last_exc:
        raise last_exc
    raise RuntimeError("post_signed failed without exception")


# --------------------------------------------------------------------
# Convenience
# --------------------------------------------------------------------

def pull(agent_id: str, limit: int = 1) -> dict:
    global _last_pull_log_ts

    body = {"agent_id": str(agent_id), "limit": int(limit), "ts": int(time.time())}

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
            print(f"[edge] ERROR pull error: {msg}")

        return {"ok": False, "commands": [], "error": msg}


def ack(agent_id: str, cmd_id: int | str, ok: bool, receipt: dict) -> dict:
    body = {
        "agent_id": str(agent_id),
        "cmd_id": cmd_id,
        "ok": bool(ok),
        "receipt": receipt or {},
        "ts": int(time.time()),
    }
    r = post_signed("/api/commands/ack", body)
    r.raise_for_status()
    return r.json()
