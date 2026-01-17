#!/usr/bin/env python3
"""
telemetry_sync.py — Phase 28 compatibility shim (KEEP HEARTBEAT, STOP LEGACY STREAM)

Goal:
- Keep legacy API surface:
    - send_heartbeat(latency_ms=0)
    - push_telemetry()
- But ensure telemetry pushes go through the Phase 28 canonical sender (telemetry_sender),
  so we do NOT emit a parallel "agent=edge" stream anymore.

This file should be safe to overwrite the existing telemetry_sync.py in the EDGE repo.
"""

import json
import os
import time
import hmac
import hashlib
from typing import Any, Dict, Optional

import requests


# ---------- Env / config ----------
AGENT_ID = os.getenv("AGENT_ID", "edge-primary")

# Heartbeat target: keep legacy env names
HB_URL = (
    (os.getenv("HEARTBEAT_URL") or os.getenv("HB_URL") or os.getenv("CLOUD_BASE_URL") or os.getenv("BASE_URL") or "")
    .rstrip("/")
)

# Secrets (tolerant)
TELEM_SECRET = (os.getenv("TELEMETRY_SECRET") or os.getenv("EDGE_SECRET") or os.getenv("OUTBOX_SECRET") or "").strip()

TIMEOUT = float(os.getenv("EDGE_HTTP_TIMEOUT", "15"))


# ---------- Helpers ----------
def _hmac_hex(key: str, raw: bytes) -> str:
    if not key:
        return ""
    return hmac.new(key.encode("utf-8"), raw, hashlib.sha256).hexdigest()


def _canonical_json_bytes(body: Dict[str, Any]) -> bytes:
    return json.dumps(body, separators=(",", ":"), sort_keys=True).encode("utf-8")


def _signature_headers(sig: str) -> Dict[str, str]:
    """
    Provide multiple signature header names for compatibility across Bus builds.
    (HTTP headers are case-insensitive; we include common variants.)
    """
    h = {"Content-Type": "application/json"}
    if not sig:
        return h

    # legacy
    h["X-Signature"] = sig
    # newer / tolerant
    h["X-OUTBOX-SIGN"] = sig
    h["X-NT-Sig"] = sig
    h["X-TELEMETRY-SIGN"] = sig
    h["X-Nova-Signature"] = sig
    h["X-NOVA-SIGNATURE"] = sig
    h["X-NOVA-SIGN"] = sig
    return h


# ---------- HEARTBEAT (kept) ----------
def send_heartbeat(latency_ms: int = 0) -> Dict[str, Any]:
    """
    Best-effort heartbeat ping.
    - Keeps the legacy endpoint /api/heartbeat if your Bus still uses it.
    - Never raises; returns {"ok": False, ...} on failure.

    IMPORTANT: We stamp agent consistently as AGENT_ID (edge-primary).
    """
    if not HB_URL or not TELEM_SECRET:
        return {"ok": False, "error": "missing HEARTBEAT_URL/HB_URL or TELEMETRY_SECRET"}

    payload = {
        "agent": AGENT_ID,
        "agent_id": AGENT_ID,
        "ts": int(time.time()),
        "latency_ms": int(latency_ms),
        "event_type": "heartbeat",
    }
    raw = _canonical_json_bytes(payload)
    sig = _hmac_hex(TELEM_SECRET, raw)

    try:
        r = requests.post(
            f"{HB_URL}/api/heartbeat",
            data=raw,
            headers=_signature_headers(sig),
            timeout=TIMEOUT,
        )
        ok = r.status_code == 200
        if ok:
            try:
                return {"ok": True, "status": r.status_code, "body": r.json()}
            except Exception:
                return {"ok": True, "status": r.status_code, "body": r.text}
        return {"ok": False, "status": r.status_code, "body": r.text}
    except Exception as e:
        return {"ok": False, "error": str(e)}


# ---------- TELEMETRY PUSH (legacy entrypoint) ----------
def push_telemetry(*args, **kwargs) -> Dict[str, Any]:
    """
    Legacy entrypoint expected by older modules.
    This MUST NOT create a second telemetry stream.

    We forward to the Phase 28 canonical telemetry sender if available.
    """
    try:
        # Primary: Phase 28 sender
        import telemetry_sender  # type: ignore

        # Prefer a "push once" style function if present
        for fn_name in ("push_balances_once", "push_telemetry_once", "push_balances", "push_telemetry"):
            fn = getattr(telemetry_sender, fn_name, None)
            if callable(fn):
                res = fn(*args, **kwargs)
                return {"ok": True, "forwarded_to": f"telemetry_sender.{fn_name}", "result": res}
        # If module imported but no compatible function
        return {"ok": False, "error": "telemetry_sender found but no push function"}
    except Exception as e:
        # Fallback: do nothing (safe), but report failure so logs can surface it
        return {"ok": False, "error": f"push_telemetry forwarding failed: {e}"}


# Back-compat alias (some callers use this name)
def push_balances(*args, **kwargs) -> Dict[str, Any]:
    return push_telemetry(*args, **kwargs)
