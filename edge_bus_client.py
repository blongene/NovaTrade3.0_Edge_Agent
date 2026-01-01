# edge_bus_client.py — Signed client for Bus command endpoints (Phase 25C auth-tolerant)
#
# Endpoints:
#   POST /api/commands/pull
#   POST /api/commands/ack
#
# Design goals:
# - Tolerate multiple base URL env var names
# - Tolerate multiple secret env var names
# - Tolerate multiple header names seen on the Bus
# - On 401, automatically try alternate (secret, header) combos
# - On 429/5xx, bounded retries with backoff
# - Normalize varying Bus response shapes for pull()

from __future__ import annotations

import os
import json
import hmac
import hashlib
import time
import random
from typing import Dict, Any, List, Tuple, Optional

import requests


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
    base = _pick_env("BUS_BASE_URL", "CLOUD_BASE_URL", "BASE_URL", "PUBLIC_BASE_URL")
    if not base:
        raise RuntimeError(
            "Bus base URL missing. Set BUS_BASE_URL (or CLOUD_BASE_URL/BASE_URL/PUBLIC_BASE_URL)."
        )
    if not (base.startswith("http://") or base.startswith("https://")):
        raise RuntimeError(f"Bus base URL invalid (must start with http/https). Got: {base!r}")
    return base.rstrip("/")


BASE_URL = _base_url()

# Timeouts/backoff
REQ_TIMEOUT = float(os.environ.get("EDGE_HTTP_TIMEOUT_SECS", "12"))     # seconds (connect/read)
RETRIES = int(os.environ.get("EDGE_HTTP_RETRIES", "4"))                 # bounded retries for transient errors
BACKOFF0 = float(os.environ.get("EDGE_HTTP_BACKOFF_SECS", "0.6"))       # base backoff
DEBUG_AUTH = (os.getenv("EDGE_DEBUG_AUTH") or "").strip().lower() in {"1", "true", "yes"}

_last_pull_log_ts = 0.0  # rate-limit noisy transient errors


# --------------------------------------------------------------------
# Signing
# --------------------------------------------------------------------

def _canonical_json(d: dict) -> str:
    # Stable HMAC input: sorted keys, compact separators
    return json.dumps(d, separators=(",", ":"), sort_keys=True)


def _sign_raw(secret: str, raw: str) -> str:
    return hmac.new(secret.encode("utf-8"), raw.encode("utf-8"), hashlib.sha256).hexdigest()


def _secret_candidates() -> List[Tuple[str, str]]:
    """
    Return ordered secret candidates as (env_name, secret_value).
    For commands pull/ack, the bus most commonly uses EDGE_SECRET (X-Nova-Signature)
    or OUTBOX_SECRET (X-OUTBOX-SIGN / X-NT-Sig), but we tolerate both.
    """
    out: List[Tuple[str, str]] = []
    seen = set()
    for env_name in ("EDGE_SECRET", "OUTBOX_SECRET", "TELEMETRY_SECRET"):
        v = (os.getenv(env_name) or "").strip()
        if v and v not in seen:
            seen.add(v)
            out.append((env_name, v))
    return out


def _auth_combos() -> List[Tuple[str, str, str]]:
    """
    Ordered auth combos: (env_name, secret, header_name)

    Priority:
      1) EDGE_SECRET with X-Nova-Signature (matches typical /api/commands/*)
      2) OUTBOX_SECRET with X-OUTBOX-SIGN
      3) OUTBOX_SECRET with X-NT-Sig
      4) EDGE_SECRET with X-OUTBOX-SIGN (tolerate)
      5) EDGE_SECRET with X-NT-Sig (tolerate)
      6) OUTBOX_SECRET with X-Nova-Signature (tolerate)
      7) TELEMETRY_SECRET fallbacks (last resort)
    """
    secrets = _secret_candidates()
    if not secrets:
        return []

    # Map env->secret for easy access
    by_env = {k: v for (k, v) in secrets}

    combos: List[Tuple[str, str, str]] = []
    if "EDGE_SECRET" in by_env:
        combos.append(("EDGE_SECRET", by_env["EDGE_SECRET"], "X-Nova-Signature"))
    if "OUTBOX_SECRET" in by_env:
        combos.append(("OUTBOX_SECRET", by_env["OUTBOX_SECRET"], "X-OUTBOX-SIGN"))
        combos.append(("OUTBOX_SECRET", by_env["OUTBOX_SECRET"], "X-NT-Sig"))

    # tolerances
    if "EDGE_SECRET" in by_env:
        combos.append(("EDGE_SECRET", by_env["EDGE_SECRET"], "X-OUTBOX-SIGN"))
        combos.append(("EDGE_SECRET", by_env["EDGE_SECRET"], "X-NT-Sig"))
    if "OUTBOX_SECRET" in by_env:
        combos.append(("OUTBOX_SECRET", by_env["OUTBOX_SECRET"], "X-Nova-Signature"))

    # last resort telemetry secret
    if "TELEMETRY_SECRET" in by_env:
        combos.append(("TELEMETRY_SECRET", by_env["TELEMETRY_SECRET"], "X-Nova-Signature"))
        combos.append(("TELEMETRY_SECRET", by_env["TELEMETRY_SECRET"], "X-OUTBOX-SIGN"))
        combos.append(("TELEMETRY_SECRET", by_env["TELEMETRY_SECRET"], "X-NT-Sig"))

    # De-dupe while preserving order
    seen = set()
    final: List[Tuple[str, str, str]] = []
    for item in combos:
        key = (item[0], item[2], item[1])
        if key not in seen:
            seen.add(key)
            final.append(item)
    return final


def _retryable_status(code: int) -> bool:
    return code == 429 or (500 <= code < 600)


def _short_text(text: str, limit: int = 200) -> str:
    t = (text or "").strip()
    if t.startswith("<!DOCTYPE") or t.startswith("<html"):
        return "HTML error page (e.g., 502/503)"
    return (t[:limit] + "…") if len(t) > limit else t


# --------------------------------------------------------------------
# Core signed POST with retries + auth fallback
# --------------------------------------------------------------------

def post_signed(
    path: str,
    body: dict,
    timeout: Optional[Tuple[float, float]] = None,
) -> requests.Response:
    """
    Signed POST with:
      - immediate auth-fallback on 401 (tries alternate header/secret combos)
      - bounded retries on 5xx/429 and connection issues
    """
    url = f"{BASE_URL}{path}"
    raw = _canonical_json(body)

    combos = _auth_combos()
    if not combos:
        raise RuntimeError("No signing secret found. Set EDGE_SECRET or OUTBOX_SECRET on Edge.")

    if timeout is None:
        timeout = (REQ_TIMEOUT, REQ_TIMEOUT)

    last_exc: Optional[Exception] = None

    # 1) Auth fallback loop: exhaust 401s quickly (no backoff).
    for env_name, secret, header_name in combos:
        sig = _sign_raw(secret, raw)
        headers = {"Content-Type": "application/json", header_name: sig}

        try:
            r = requests.post(url, data=raw, headers=headers, timeout=timeout)

            if r.status_code == 401:
                # Try next combo
                last_exc = requests.HTTPError(f"401 {_short_text(r.text)}", response=r)
                continue

            if _retryable_status(r.status_code):
                # Transient -> go to retry loop using THIS combo (best guess)
                last_exc = requests.HTTPError(f"{r.status_code} {_short_text(r.text)}", response=r)
                return _retry_with_combo(url, raw, headers, timeout, last_exc)

            r.raise_for_status()

            if DEBUG_AUTH:
                print(f"[edge][auth] OK {path} using {env_name} + {header_name}")

            return r

        except (requests.Timeout, requests.ConnectionError) as e:
            last_exc = e
            # retry with first combo (or current) via retry loop
            return _retry_with_combo(url, raw, headers, timeout, last_exc)

        except requests.HTTPError as e:
            resp = getattr(e, "response", None)
            code = getattr(resp, "status_code", None)
            if code == 401:
                last_exc = e
                continue
            if code is None or not _retryable_status(int(code)):
                raise
            last_exc = e
            return _retry_with_combo(url, raw, headers, timeout, last_exc)

    # If we exhausted combos, surface the clearest 401.
    if last_exc:
        raise last_exc
    raise RuntimeError("post_signed failed without exception")


def _retry_with_combo(
    url: str,
    raw: str,
    headers: Dict[str, str],
    timeout: Tuple[float, float],
    last_exc: Exception,
) -> requests.Response:
    # 2) Bounded retry loop for transient errors (429/5xx/timeouts).
    exc: Exception = last_exc
    for attempt in range(1, RETRIES + 1):
        try:
            r = requests.post(url, data=raw, headers=headers, timeout=timeout)

            if r.status_code == 401:
                r.raise_for_status()  # don't retry auth problems here

            if _retryable_status(r.status_code):
                exc = requests.HTTPError(f"{r.status_code} {_short_text(r.text)}", response=r)
            else:
                r.raise_for_status()
                return r

        except (requests.Timeout, requests.ConnectionError) as e:
            exc = e

        except requests.HTTPError as e:
            resp = getattr(e, "response", None)
            code = getattr(resp, "status_code", None)
            if code is None or not _retryable_status(int(code)):
                raise
            exc = e

        sleep_s = BACKOFF0 * (2 ** (attempt - 1)) + random.uniform(0, 0.25)
        time.sleep(min(sleep_s, 6.0))

    raise exc


# --------------------------------------------------------------------
# Response normalization
# --------------------------------------------------------------------

def _normalize_pull_response(res: Any) -> Dict[str, Any]:
    """
    Normalize to:
      {"ok": bool, "commands": [ { "id": ..., "intent": {...} }, ... ], "raw": <original> }
    Handles common shapes:
      - {"ok":true, "commands":[...]}
      - {"ok":true, "items":[...]}
      - {"items":[...]}
      - {"commands":[...]}
    """
    out: Dict[str, Any] = {"ok": False, "commands": [], "raw": res}

    if isinstance(res, dict):
        ok = bool(res.get("ok", True))  # if no ok key, treat as ok-ish
        cmds = res.get("commands")
        if cmds is None:
            cmds = res.get("items")
        if cmds is None:
            cmds = res.get("leased")
        if cmds is None:
            cmds = []
        if isinstance(cmds, list):
            out["ok"] = ok
            out["commands"] = cmds
            return out

    # unknown shape
    return out


# --------------------------------------------------------------------
# Convenience
# --------------------------------------------------------------------

def pull(agent_id: str, limit: int = 1) -> dict:
    """
    Pull/lease commands for this agent. Returns normalized dict with `commands` list.
    """
    global _last_pull_log_ts

    body = {
        "agent_id": str(agent_id).strip(),
        "limit": int(limit),
        "max_items": int(limit),  # tolerate bus implementations expecting max_items
        "n": int(limit),          # tolerate alternate key
        "ts": int(time.time()),
    }

    try:
        r = post_signed("/api/commands/pull", body)
        data = r.json()
        return _normalize_pull_response(data)
    except Exception as e:
        msg = getattr(e, "args", [str(e)])[0]
        code = getattr(getattr(e, "response", None), "status_code", None)
        now = time.time()

        transient = (code in (502, 503, 504, 429)) or ("HTML error page" in str(msg))
        if transient:
            if now - _last_pull_log_ts > 60:
                print(f"[edge] WARN pull transient {code or ''}: {msg}")
                _last_pull_log_ts = now
        else:
            print(f"[edge] ERROR pull error: {code or ''} {msg}")

        return {"ok": False, "commands": [], "error": msg}


def ack(agent_id: str, cmd_id: int | str, ok: bool, receipt: dict) -> dict:
    """
    ACK command completion back to the bus.
    """
    body = {
        "agent_id": str(agent_id).strip(),
        "cmd_id": cmd_id,
        "ok": bool(ok),
        "receipt": receipt or {},
        "ts": int(time.time()),
    }
    r = post_signed("/api/commands/ack", body)
    r.raise_for_status()
    return r.json()
