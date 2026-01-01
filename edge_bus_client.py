#!/usr/bin/env python3
"""
edge_bus_client.py
Client for Edge <-> Bus command leasing endpoints.

Bus expects for /api/commands/pull and /api/commands/ack:
  HMAC(secret=OUTBOX_SECRET, body=canonical_json) in header X-OUTBOX-SIGN

This fixes Phase 25C 401 Unauthorized by signing correctly.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import time
from typing import Any, Dict, Optional

import requests


DEFAULT_PULL_PATH = "/api/commands/pull"
DEFAULT_ACK_PATH  = "/api/commands/ack"

DEFAULT_SIGN_HEADER = "X-OUTBOX-SIGN"
DEFAULT_SECRET_ENV  = "OUTBOX_SECRET"


def _canonical_json(obj: Any) -> str:
    # Stable JSON for HMAC: sort keys + compact separators
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _get_bus_base_url() -> str:
    base = (
        os.getenv("BUS_BASE_URL")
        or os.getenv("CLOUD_BASE_URL")
        or os.getenv("PUBLIC_BASE_URL")
        or os.getenv("BASE_URL")
        or ""
    ).strip()
    if not base:
        raise RuntimeError(
            "Bus base URL is not set. Set BUS_BASE_URL or CLOUD_BASE_URL "
            "(e.g. https://novatrade3-0.onrender.com)."
        )
    return base.rstrip("/")


def _get_outbox_secret() -> str:
    # Commands pull/ack should be OUTBOX_SECRET. If missing, fail loudly.
    secret = (os.getenv(DEFAULT_SECRET_ENV) or "").strip()
    if not secret:
        raise RuntimeError(
            "OUTBOX_SECRET is not set on Edge. "
            "Phase 25C command leasing requires OUTBOX_SECRET to sign /api/commands/*."
        )
    return secret


def _sign(secret: str, body_text: str) -> str:
    return hmac.new(secret.encode("utf-8"), body_text.encode("utf-8"), hashlib.sha256).hexdigest()


def post_signed(
    path: str,
    payload: Dict[str, Any],
    *,
    timeout: int = 15,
    session: Optional[requests.Session] = None,
) -> requests.Response:
    """
    POST JSON with OUTBOX HMAC signature header.
    """
    base = _get_bus_base_url()
    url = base + path

    # include a timestamp to discourage replay + make debugging easier
    if "ts" not in payload:
        payload["ts"] = int(time.time())

    body_text = _canonical_json(payload)
    secret = _get_outbox_secret()
    sig = _sign(secret, body_text)

    headers = {
        "Content-Type": "application/json",
        DEFAULT_SIGN_HEADER: sig,
    }

    s = session or requests.Session()
    resp = s.post(url, data=body_text.encode("utf-8"), headers=headers, timeout=timeout)

    # Helpful hint (no secrets) on auth failures
    if resp.status_code in (401, 403):
        try:
            j = resp.json()
        except Exception:
            j = {"raw": resp.text[:200]}
        print(
            f"[edge_bus_client] AUTH FAIL {resp.status_code} on {path} "
            f"(expects {DEFAULT_SECRET_ENV}+{DEFAULT_SIGN_HEADER}). "
            f"resp={j}"
        )

    return resp


def pull(agent_id: str, limit: int = 1) -> Dict[str, Any]:
    """
    Pull/lease commands for this agent.
    Returns dict parsed from JSON.
    """
    payload = {"agent_id": str(agent_id).strip(), "limit": int(limit)}
    r = post_signed(DEFAULT_PULL_PATH, payload)
    try:
        return r.json()
    except Exception:
        return {"ok": False, "status": r.status_code, "text": r.text[:500]}


def ack(agent_id: str, cmd_id: str, ok: bool, receipt: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Ack a leased command back to Bus.
    """
    payload: Dict[str, Any] = {
        "agent_id": str(agent_id).strip(),
        "cmd_id": str(cmd_id),
        "ok": bool(ok),
    }
    if receipt is not None:
        payload["receipt"] = receipt

    r = post_signed(DEFAULT_ACK_PATH, payload)
    try:
        return r.json()
    except Exception:
        return {"ok": False, "status": r.status_code, "text": r.text[:500]}
