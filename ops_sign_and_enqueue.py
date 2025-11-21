# ops_sign_and_enqueue.py
#
# Dual-use module:
#   1) Bus helper for nova_trigger
#   2) CLI helper
#
# FIXES APPLIED:
# - Enforces sort_keys=True (Canonical JSON) to match Bus verification.
# - Sends X-Nova-Signature to match Bus expectations.

from __future__ import annotations

import argparse
import hashlib
import hmac
import json
import os
import sys
import time
from typing import Any, Dict, Optional, Tuple

import requests

# ---------- HMAC helpers ----------

def _load_secret_from_env() -> bytes:
    """Load OUTBOX_SECRET from either OUTBOX_SECRET or OUTBOX_SECRET_FILE."""
    s = os.getenv("OUTBOX_SECRET") or ""
    path = os.getenv("OUTBOX_SECRET_FILE")
    if not s and path and os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            s = f.read().strip()
    if not s:
        raise RuntimeError("OUTBOX_SECRET or OUTBOX_SECRET_FILE must be set")
    return s.encode("utf-8")

def hmac_hex(secret: bytes, data: bytes) -> str:
    return hmac.new(secret, data, hashlib.sha256).hexdigest()

def _trial_signatures(secret: bytes, raw_json: bytes, ts: str):
    """Yield (label, headers) for the various signing schemes."""
    body_str = raw_json.decode()
    
    # PRIMARY: Payload only (Canonical) - Matches wsgi.py _verify_sorted
    sig_body = hmac_hex(secret, raw_json)
    
    trials = [
        ("body", sig_body),
        ("ts+body", hmac_hex(secret, (ts + body_str).encode())),
        ("body+ts", hmac_hex(secret, (body_str + ts).encode())),
    ]
    
    for label, sig in trials:
        yield label, {
            "Content-Type": "application/json",
            "X-Timestamp": ts,
            "X-Nova-Signature": sig,    # Modern header
            "X-Outbox-Signature": sig,  # Legacy backup
            "X-Signature": sig,         # Legacy backup
        }

def _attempt_raw(
    url: str,
    secret: bytes,
    body_dict: Dict[str, Any],
    *,
    timeout: float = 15.0,
    verbose: bool = True,
) -> Tuple[bool, Optional[str], Optional[Dict[str, Any]], Optional[int]]:
    """Core HTTP logic. Tries multiple signing headers until one works."""
    
    # CRITICAL FIX: sort_keys=True ensures we send the exact same byte sequence
    # that the Bus expects for its "Canonical" verification.
    raw_json = json.dumps(
        body_dict, 
        separators=(",", ":"), 
        sort_keys=True, 
        ensure_ascii=False
    ).encode()
    
    ts = str(int(time.time()))
    last_status: Optional[int] = None
    last_body: Optional[Dict[str, Any]] = None

    for label, headers in _trial_signatures(secret, raw_json, ts):
        try:
            r = requests.post(url, data=raw_json, headers=headers, timeout=timeout)
            last_status = r.status_code
            try:
                last_body = r.json()
            except Exception:
                last_body = {"raw": r.text}
            
            if verbose:
                print(f"[{label}] status={r.status_code} body={last_body}")
            
            # 200-299 = Success
            if 200 <= r.status_code < 300:
                return True, label, last_body, last_status
            # 401/403 = Auth failed, loop might try next sig format
            # 400/500 = Logic/Server error, likely definitive
            if r.status_code >= 400 and r.status_code != 401 and r.status_code != 403:
                 return False, label, last_body, last_status

        except Exception as e:
            if verbose:
                print(f"[{label}] request failed: {e}")
            last_body = {"error": str(e)}

    return False, "all_failed", last_body, last_status

# ---------- Envelope → payload ----------

def _derive_base_from_env() -> str:
    base = os.getenv("OPS_ENQUEUE_BASE", "").strip()
    if not base:
        base = os.getenv("OPS_BASE_URL", "").strip()
    if base:
        return base.rstrip("/")

    url = os.getenv("OPS_ENQUEUE_URL", "").strip()
    if not url:
        raise RuntimeError("OPS_ENQUEUE_BASE, OPS_BASE_URL or OPS_ENQUEUE_URL must be set")

    for suffix in ("/api/ops/enqueue", "/ops/enqueue"):
        if url.endswith(suffix):
            url = url[: -len(suffix)]
            break
    if url.endswith("/api"):
        url = url[:-4]
    return url.rstrip("/")

def _shape_body_from_envelope(envelope: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(envelope, dict):
        raise RuntimeError("envelope must be a dict")

    agent_id = envelope.get("agent_id") or "edge-primary"

    if "payload" in envelope and "type" in envelope:
        body = {
            "agent_id": agent_id,
            "type": envelope["type"],
            "payload": envelope["payload"],
        }
        if "meta" in envelope:
            body["meta"] = envelope["meta"]
        return body

    intent: Dict[str, Any] = envelope.get("intent") or {}
    cmd_type = intent.get("type") or "order.place"

    body = {
        "agent_id": agent_id,
        "type": cmd_type,
        "payload": intent,
    }
    meta = envelope.get("meta")
    if isinstance(meta, dict):
        body["meta"] = meta
    return body

def attempt(envelope: Dict[str, Any], *, timeout: float = 15.0) -> Dict[str, Any]:
    try:
        base = _derive_base_from_env()
        url = base.rstrip("/") + "/ops/enqueue"
        secret = _load_secret_from_env()
        payload = _shape_body_from_envelope(envelope)
        body_dict = {"payload": payload}
    except RuntimeError as e:
        return {"ok": False, "reason": str(e), "status": None}

    ok, label, j, status = _attempt_raw(url, secret, body_dict, timeout=timeout, verbose=False)

    if not ok:
        return {
            "ok": False,
            "reason": f"enqueue_failed (label={label}, status={status})",
            "status": status,
        }

    reason = j.get("reason") if isinstance(j, dict) else "ok"
    return {"ok": True, "reason": reason or "ok", "status": status}

# ---------- CLI entrypoint ----------

def cli_attempt(base: str, secret_str: str, payload: Dict[str, Any]) -> None:
    secret = secret_str.encode()
    url = base.rstrip("/") + "/ops/enqueue"
    body_dict = {"payload": payload}
    ok, label, j, status = _attempt_raw(url, secret, body_dict, timeout=15.0, verbose=True)
    if not ok:
        print("All signing patterns failed. Check OUTBOX_SECRET, time sync, and that /ops/enqueue is deployed.")
        sys.exit(2)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", required=True, help="Base URL")
    ap.add_argument("--secret", required=True, help="Outbox secret")
    ap.add_argument("--agent", default="edge-primary")
    ap.add_argument("--venue", required=True)
    ap.add_argument("--symbol", required=True)
    ap.add_argument("--side", required=True, choices=["BUY", "SELL"])
    ap.add_argument("--amount", required=True)
    ap.add_argument("--tif", default="IOC")
    args = ap.parse_args()

    body = {
        "agent_id": args.agent,
        "type": "order.place",
        "payload": {
            "venue": args.venue,
            "symbol": args.symbol,
            "side": args.side,
            "amount": str(args.amount),
            "time_in_force": args.tif,
        },
    }
    cli_attempt(args.base, args.secret, body)

if __name__ == "__main__":
    main()
