#!/usr/bin/env python3
"""
receipt_bus.py — Edge → Bus receipts client (deterministic HMAC, retries, schema guards)

Purpose:
  Post normalized trade receipts from the Edge Agent to the Cloud Bus so the Bus
  (single writer) can upsert Trade_Log / telemetry.

Contract:
  - HMAC header: X-Nova-Signature  (secret = EDGE_SECRET)
  - Endpoint:    {CLOUD_BASE_URL}/api/receipts/ack
  - JSON body (sorted for HMAC):
      {
        "agent_id": "<AGENT_ID>",
        "cmd_id":   "<command id from bus>",
        "ts":       <client ms since epoch>,
        "normalized": {
          "venue": "COINBASE|BINANCEUS|KRAKEN|MEXC",
          "symbol": "BASE/QUOTE",          # e.g., BTC/USDC
          "side": "BUY|SELL",
          "mode": "MARKET|LIMIT",
          "status": "ok|rejected|error|partial",
          "order_id": "<venue order id>",
          "client_id": "<our idempotency key or None>",
          "base_filled": <float>,          # e.g., 0.00009
          "quote_filled": <float>,         # e.g., 10.01
          "fee": <float>,                  # total fee in fee_asset
          "fee_asset": "USDT|USDC|BTC|...",
          "tx_ts": <venue ms since epoch>  # optional but recommended
        },
        "raw": {...}  # raw venue payload (safe subset), optional but helpful
      }

Environment variables (Edge):
  - CLOUD_BASE_URL  (required) e.g., https://novatrade3-0.onrender.com
  - EDGE_SECRET     (required) hex secret for HMAC
  - AGENT_ID        (required) e.g., edge-cb-1
  - RECEIPTS_PATH   (optional) defaults to /api/receipts/ack
  - TIMEOUT_S       (optional) default 20
  - RETRIES         (optional) default 3
  - BACKOFF_S       (optional) base backoff seconds, default 0.5
"""

from __future__ import annotations
import os, time, json, hmac, hashlib, math
from typing import Any, Dict, Optional, Mapping
import requests

# ---------- Env & Defaults ----------
CLOUD_BASE_URL = (os.getenv("CLOUD_BASE_URL") or "").rstrip("/")
EDGE_SECRET    = os.getenv("EDGE_SECRET") or ""
AGENT_ID       = os.getenv("AGENT_ID") or ""
RECEIPTS_PATH  = os.getenv("RECEIPTS_PATH") or "/api/receipts/ack"
TIMEOUT_S      = float(os.getenv("TIMEOUT_S") or "20")
RETRIES        = int(os.getenv("RETRIES") or "3")
BACKOFF_S      = float(os.getenv("BACKOFF_S") or "0.5")
MAX_BODY_BYTES = 64 * 1024  # 64 KiB cap for safety

# ---------- Simple logger ----------
def _log(msg: str) -> None:
    ts = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
    print(f"[receipt_bus] {ts}Z {msg}", flush=True)

# ---------- HMAC helpers ----------
def _hmac_hex(secret: str, raw: bytes) -> str:
    return hmac.new(secret.encode("utf-8"), raw, hashlib.sha256).hexdigest()

# ---------- Validation ----------
_REQUIRED_NORMALIZED = (
    "venue", "symbol", "side", "mode", "status", "order_id",
)

def _validate_env() -> None:
    missing = []
    if not CLOUD_BASE_URL: missing.append("CLOUD_BASE_URL")
    if not EDGE_SECRET:    missing.append("EDGE_SECRET")
    if not AGENT_ID:       missing.append("AGENT_ID")
    if missing:
        raise RuntimeError(f"Missing required env: {', '.join(missing)}")

def _validate_payload(payload: Mapping[str, Any]) -> None:
    if not isinstance(payload, Mapping):
        raise ValueError("payload must be a mapping")
    if "agent_id" not in payload or not payload["agent_id"]:
        raise ValueError("payload.agent_id required")
    if "cmd_id" not in payload or payload["cmd_id"] in (None, ""):
        raise ValueError("payload.cmd_id required")
    if "normalized" not in payload or not isinstance(payload["normalized"], Mapping):
        raise ValueError("payload.normalized required")
    norm = payload["normalized"]
    missing = [k for k in _REQUIRED_NORMALIZED if k not in norm]
    if missing:
        raise ValueError(f"normalized missing keys: {', '.join(missing)}")

    # numeric sanity
    for k in ("base_filled", "quote_filled", "fee"):
        if k in norm and norm[k] is not None:
            try:
                float(norm[k])
            except Exception:
                raise ValueError(f"normalized.{k} must be numeric if present")

    # SELL/BUY consistency (soft check; Bus enforces too)
    side = str(norm.get("side", "")).upper()
    if side == "SELL":
        if float(norm.get("base_filled") or 0.0) < 0 and float(norm.get("quote_filled") or 0.0) > 0:
            raise ValueError("normalized.base_filled must be >= 0")
    elif side == "BUY":
        # allow base_filled/quote_filled == 0 for open/partial
        pass

def _coerce_numbers(norm: Dict[str, Any]) -> None:
    # Make sure numerics are floats in outgoing JSON (helps the Bus writer)
    for k in ("base_filled", "quote_filled", "fee"):
        if k in norm and norm[k] is not None:
            norm[k] = float(norm[k])

# ---------- Core sender ----------
def send_receipt(
    *,
    cmd_id: str,
    normalized: Dict[str, Any],
    raw: Optional[Dict[str, Any]] = None,
    agent_id: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Send a single receipt to the Bus. Returns dict with {ok: bool, status: int, body: Any}.
    Will retry on 5xx/timeout up to RETRIES.
    """
    _validate_env()

    agent = agent_id or AGENT_ID
    now_ms = int(time.time() * 1000)

    # Prepare body
    body: Dict[str, Any] = {
        "agent_id": agent,
        "cmd_id":   str(cmd_id),
        "ts":       now_ms,
        "normalized": dict(normalized or {}),
    }
    if raw:
        # Keep raw compact; avoid massive blobs
        body["raw"] = raw

    # Normalize/validate numbers and schema
    _coerce_numbers(body["normalized"])
    _validate_payload(body)

    # Deterministic JSON for HMAC (MUST match server verify)
    body_sorted = json.dumps(body, separators=(",", ":"), sort_keys=True, ensure_ascii=False)
    raw_bytes   = body_sorted.encode("utf-8")

    if len(raw_bytes) > MAX_BODY_BYTES:
        raise ValueError(f"receipt body too large ({len(raw_bytes)} bytes > {MAX_BODY_BYTES})")

    sig = _hmac_hex(EDGE_SECRET, raw_bytes)
    url = f"{CLOUD_BASE_URL}{RECEIPTS_PATH}"
    headers = {
        "Content-Type": "application/json",
        "X-Nova-Signature": sig,
        # Optional: stamp for server logs (no secrets)
        "X-Nova-Agent": agent,
    }

    # Transient retry loop
    attempts = max(1, int(RETRIES))
    backoff  = max(0.0, float(BACKOFF_S))
    last_err: Optional[str] = None

    for i in range(1, attempts + 1):
        try:
            r = requests.post(url, data=raw_bytes, headers=headers, timeout=TIMEOUT_S)
            ct = (r.headers.get("content-type") or "")
            body_resp: Any
            try:
                body_resp = r.json() if "json" in ct else r.text
            except Exception:
                body_resp = r.text

            if 200 <= r.status_code < 300:
                _log(f"ack ok cmd_id={cmd_id} http={r.status_code}")
                return {"ok": True, "status": r.status_code, "body": body_resp}

            # Non-2xx
            _log(f"ack http={r.status_code} cmd_id={cmd_id} body={str(body_resp)[:300]}")
            # Retry only on 5xx
            if 500 <= r.status_code < 600 and i < attempts:
                time.sleep(backoff * (2 ** (i - 1)))
                continue
            return {"ok": False, "status": r.status_code, "body": body_resp}

        except requests.Timeout:
            last_err = "timeout"
            _log(f"ack timeout cmd_id={cmd_id} attempt={i}/{attempts}")
            if i < attempts:
                time.sleep(backoff * (2 ** (i - 1)))
                continue
            return {"ok": False, "status": 599, "body": {"error": "timeout"}}

        except Exception as e:
            last_err = f"{type(e).__name__}: {e}"
            _log(f"ack error cmd_id={cmd_id} attempt={i}/{attempts} err={last_err}")
            if i < attempts:
                time.sleep(backoff * (2 ** (i - 1)))
                continue
            return {"ok": False, "status": 598, "body": {"error": str(e)}}

    # Should not reach
    return {"ok": False, "status": 598, "body": {"error": last_err or "unknown"}}

# ---------- Optional CLI (for quick manual tests) ----------
if __name__ == "__main__":
    # Example: python receipt_bus.py
    # (Builds a minimal BUY-ok receipt to test the pipe.)
    try:
        _validate_env()
    except Exception as e:
        _log(f"env error: {e}")
        raise

    example = {
        "venue": "BINANCEUS",
        "symbol": "BTC/USDT",
        "side": "BUY",
        "mode": "MARKET",
        "status": "ok",
        "order_id": "demo-oid-123",
        "client_id": "demo-cid-123",
        "base_filled": 0.00009,
        "quote_filled": 10.01,
        "fee": 0.01,
        "fee_asset": "USDT",
        "tx_ts": int(time.time() * 1000),
    }
    res = send_receipt(
        cmd_id="demo-cmd-123",
        normalized=example,
        raw={"note": "demo test only"},
    )
    _log(f"test result: {res}")
