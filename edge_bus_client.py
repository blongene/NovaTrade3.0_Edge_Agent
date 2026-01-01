# edge_bus_client.py — Signed client for Bus command endpoints
# - Uses canonical JSON signing (sorted keys, compact separators)
# - Supports retry/backoff on 429/5xx and timeouts
# - Auth-fallback loop to quickly try alternate (secret, header) combos

import os
import json
import time
import random
import hmac
import hashlib
from typing import Any, Dict, List, Optional, Tuple

import requests


# --------------------------------------------------------------------
# Config
# --------------------------------------------------------------------

REQ_TIMEOUT = float(os.getenv("EDGE_BUS_REQ_TIMEOUT", "10"))
RETRIES = int(os.getenv("EDGE_BUS_RETRIES", "3"))
BACKOFF0 = float(os.getenv("EDGE_BUS_BACKOFF0", "0.6"))

BASE_URL = (os.getenv("CLOUD_BASE_URL") or os.getenv("BUS_BASE_URL") or os.getenv("BASE_URL") or "").rstrip("/")


# --------------------------------------------------------------------
# Env helpers
# --------------------------------------------------------------------

def _pick_env(name: str) -> Optional[str]:
    v = (os.getenv(name) or "").strip()
    return v or None


def _base_url() -> str:
    b = (os.getenv("CLOUD_BASE_URL") or os.getenv("BUS_BASE_URL") or os.getenv("BASE_URL") or "").rstrip("/")
    if not b:
        raise RuntimeError("Bus base URL not configured. Set CLOUD_BASE_URL or BUS_BASE_URL on Edge.")
    return b


# --------------------------------------------------------------------
# Signing
# --------------------------------------------------------------------

def _canonical_json(d: dict) -> str:
    # Stable HMAC input: sorted keys, compact separators
    return json.dumps(d, separators=(",", ":"), sort_keys=True)


def _sign_raw(secret: str, raw: str) -> str:
    return hmac.new(secret.encode("utf-8"), raw.encode("utf-8"), hashlib.sha256).hexdigest()


def _signed_headers(sig: str, primary_header: str) -> Dict[str, str]:
    """Return request headers carrying the same HMAC under multiple header names.

    The Bus has historically accepted several signature header names across phases.
    We send the signature under a compatibility set so Edge can talk to mixed Bus builds.
    `primary_header` is included for readability and to preserve the intent of auth-combos.
    """
    base = {"Content-Type": "application/json"}

    # Canonical (Phase 24/25)
    base["X-OUTBOX-SIGN"] = sig

    # Legacy/compat variants (case-insensitive on the wire)
    base["X-TELEMETRY-SIGN"] = sig
    base["X-Nova-Signature"] = sig
    base["X-NOVA-SIGNATURE"] = sig
    base["X-NOVA-SIGN"] = sig
    base["X-NT-Sig"] = sig

    # Ensure the combo's primary header is present (even if unusual casing)
    if primary_header and primary_header not in base:
        base[primary_header] = sig
    return base


def _secret_candidates() -> List[Tuple[str, str]]:
    """
    Return ordered secret candidates as (env_name, secret_value).
    For commands pull/ack, the bus most commonly uses EDGE_SECRET (X-Nova-Signature)
    or OUTBOX_SECRET (X-OUTBOX-SIGN). TELEMETRY_SECRET is included as a last-resort fallback.
    """
    out: List[Tuple[str, str]] = []
    for env in ("EDGE_SECRET", "OUTBOX_SECRET", "TELEMETRY_SECRET"):
        v = _pick_env(env)
        if v:
            out.append((env, v))
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
    by_env = {k: v for k, v in secrets}

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
        combos.append(("TELEMETRY_SECRET", by_env["TELEMETRY_SECRET"], "X-OUTBOX-SIGN"))
        combos.append(("TELEMETRY_SECRET", by_env["TELEMETRY_SECRET"], "X-Nova-Signature"))
        combos.append(("TELEMETRY_SECRET", by_env["TELEMETRY_SECRET"], "X-NT-Sig"))

    return combos


# --------------------------------------------------------------------
# Retry logic
# --------------------------------------------------------------------

def _retryable_status(code: int) -> bool:
    return int(code) in (408, 409, 425, 429, 500, 502, 503, 504)


def _short_text(t: str, n: int = 300) -> str:
    try:
        t = (t or "").strip()
    except Exception:
        return ""
    if len(t) <= n:
        return t
    return t[:n] + "…"


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
    base = _base_url()
    url = f"{base}{path}"
    raw = _canonical_json(body)

    combos = _auth_combos()
    if not combos:
        raise RuntimeError("No signing secret found. Set EDGE_SECRET or OUTBOX_SECRET (or TELEMETRY_SECRET as fallback) on Edge.")

    if timeout is None:
        timeout = (REQ_TIMEOUT, REQ_TIMEOUT)

    last_exc: Optional[Exception] = None

    # 1) Auth fallback loop: exhaust 401s quickly (no backoff).
    for env_name, secret, header_name in combos:
        sig = _sign_raw(secret, raw)
        headers = _signed_headers(sig, header_name)

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
            return r

        except (requests.Timeout, requests.ConnectionError) as e:
            last_exc = e
            # go to bounded retry loop with this combo
            return _retry_with_combo(url, raw, headers, timeout, e)

        except requests.HTTPError as e:
            # Non-401 auth errors should not be retried across secrets
            raise

    # If we exhausted combos due to 401s only
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
        time.sleep(min(10.0, sleep_s))

    raise exc


# --------------------------------------------------------------------
# Response normalization for pull
# --------------------------------------------------------------------

def _normalize_pull_response(res: Any) -> Dict[str, Any]:
    """
    Normalize possible bus responses:
      - {"ok":true, "commands":[...]}
      - {"ok":true, "items":[...]}
      - {"items":[...]}
      - {"commands":[...]}
      - {"leased":[...]}
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
        if not isinstance(cmds, list):
            cmds = []
        out["ok"] = ok
        out["commands"] = cmds
        return out

    # If bus returns list directly
    if isinstance(res, list):
        out["ok"] = True
        out["commands"] = res
        return out

    return out


# --------------------------------------------------------------------
# Public API: pull + ack
# --------------------------------------------------------------------

_last_pull_log_ts = 0.0


def pull(agent_id: str, limit: int = 5, log_every_s: int = 60) -> List[dict]:
    """
    Pull leased commands from Bus.
    """
    global _last_pull_log_ts

    body = {
        "agent_id": str(agent_id).strip(),
        "limit": int(limit),
        "max_items": int(limit),  # tolerate bus implementations expecting max_items
        "n": int(limit),          # tolerate alternate key
        "ts": int(time.time()),
    }

    r = post_signed("/api/commands/pull", body)
    data = None
    try:
        data = r.json()
    except Exception:
        data = {"ok": False, "error": "non-json", "raw_text": r.text}

    norm = _normalize_pull_response(data)
    cmds = norm.get("commands") or []

    now = time.time()
    if log_every_s and (now - _last_pull_log_ts) >= float(log_every_s):
        _last_pull_log_ts = now
        print(f"[edge_bus_client] pull ok={norm.get('ok')} n={len(cmds)}")

    return cmds


def ack(agent_id: str, cmd_id: Any, status: str, receipt: dict) -> dict:
    """
    ACK a command back to Bus with receipt.
    """
    body = {
        "agent_id": str(agent_id).strip(),
        "id": cmd_id,
        "status": status,
        "receipt": receipt or {},
        "ts": int(time.time()),
    }

    r = post_signed("/api/commands/ack", body)
    try:
        return r.json()
    except Exception:
        return {"ok": False, "error": "non-json", "raw_text": r.text}
