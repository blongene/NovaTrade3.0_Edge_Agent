#!/usr/bin/env python3
"""
NovaTrade 3.0 Edge Agent (unified, HMAC-signed)

- Long-polls the Bus /api/commands/pull endpoint for commands.
- Executes trading intents on venue-specific executors.
- ACKs results (or errors/holds) back to /api/commands/ack.
- Pushes balance telemetry in a background thread.

This file is a DROP-IN replacement for edge_agent.py.
"""

import json
import logging
import os
import time
import hmac
import hashlib
from typing import Any, Dict, List, Optional

import importlib
import requests

from telemetry_sender import start_balance_pusher  # existing helper

# ---------- Config ----------

BASE_URL = (
    (os.getenv("CLOUD_BASE_URL") or os.getenv("BASE_URL") or "").rstrip("/")
    or "https://novatrade3-0.onrender.com"
)
EDGE_SECRET = os.getenv("EDGE_SECRET", "")
AGENT_ID    = os.getenv("AGENT_ID", "edge-primary")

EDGE_MODE = os.getenv("EDGE_MODE", "live").lower()   # "live" or "dryrun"
EDGE_HOLD = os.getenv("EDGE_HOLD", "false").lower() == "true"

POLL_SECS = float(os.getenv("EDGE_POLL_SECS", "5"))
TIMEOUT   = float(os.getenv("EDGE_HTTP_TIMEOUT", "15"))

LOG_LEVEL = os.getenv("EDGE_LOG_LEVEL", "INFO").upper()

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="[edge] %(levelname)s %(asctime)s %(message)s",
)
log = logging.getLogger("edge")


# ---------- HMAC + HTTP helpers ----------

def _hmac_sha256_hex(key: str, raw: bytes) -> str:
    if not key:
        return ""
    return hmac.new(key.encode("utf-8"), raw, hashlib.sha256).hexdigest()


def _post_json(path: str, body: Dict[str, Any]) -> Any:
    raw = json.dumps(body, separators=(",", ":"), sort_keys=True).encode("utf-8")
    sig = _hmac_sha256_hex(EDGE_SECRET, raw)

    url = f"{BASE_URL}{path}"
    headers = {
        "Content-Type": "application/json",
    }
    if sig:
        # Must match ops_api_sqlite.py header_name exactly
        headers["X-Outbox-Signature"] = sig

    r = requests.post(url, data=raw, headers=headers, timeout=TIMEOUT)
    ...

    r = requests.post(url, data=raw, headers=headers, timeout=TIMEOUT)
    if r.status_code >= 400:
        raise requests.HTTPError(f"{r.status_code} {r.text}")
    # Some endpoints return plain values, so guard json()
    try:
        return r.json()
    except Exception:
        return None


def bus_pull(lease_limit: int = 1) -> Any:
    """
    Pull NEW commands from /api/commands/pull.

    Body is currently {"limit": N}. agent_id is ignored by the Bus but
    included for future compatibility.
    """
    body = {
        "agent_id": AGENT_ID,
        "limit": int(lease_limit),
    }
    return _post_json("/api/commands/pull", body)


def bus_ack(cmd_id: int, ok: bool, receipt: Dict[str, Any], *, status: Optional[str] = None) -> Any:
    """
    Send execution result back to the bus.

    /api/commands/ack accepts:
      {
        "id": <cmd_id>,
        "agent_id": "...",
        "status": "done" | "error" | "held",
        "receipt": {...}
      }
    """
    state = status or ("done" if ok else "error")
    body: Dict[str, Any] = {
        "id": int(cmd_id),
        "agent_id": AGENT_ID,
        "status": state,
        "receipt": receipt,
    }
    return _post_json("/api/commands/ack", body)


# ---------- Venue resolver ----------

def import_exec_for_venue(venue: str):
    """
    Return the venue-specific executor callable.

    Each executor module must expose:

        def execute_market_order(intent: Dict[str, Any]) -> Dict[str, Any]:
            ...

    We keep a thin mapping from venue name → module path.
    """
    module_map = {
        "KRAKEN":    "executors.kraken_executor",
        "BINANCEUS": "executors.binance_us_executor",
        # Coinbase uses the advanced executor variant
        "COINBASE":  "executors.coinbase_advanced_executor",
    }
    mod_name = module_map.get((venue or "").upper())
    if not mod_name:
        raise ImportError(f"Unsupported venue: {venue!r}")

    mod = importlib.import_module(mod_name)
    fn = getattr(mod, "execute_market_order", None)
    if not callable(fn):
        raise ImportError(f"{mod_name} missing execute_market_order()")
    return fn


# ---------- Intent / receipt helpers ----------

def _simulate_receipt(intent: Dict[str, Any], *, reason: str = "simulated") -> Dict[str, Any]:
    venue  = (intent.get("venue") or "").upper()
    symbol = intent.get("symbol") or intent.get("pair") or ""
    side   = (intent.get("side") or "BUY").upper()
    amt    = float(intent.get("amount_usd") or intent.get("amount") or 0)

    return {
        "normalized": True,
        "ok": True,
        "status": "simulated",
        "venue": venue,
        "symbol": symbol,
        "side": side,
        "amount_usd": amt,
        "reason": reason,
        "raw_intent": intent,
    }


def _normalize_receipt(ok: bool, venue: str, symbol: str, raw: Dict[str, Any]) -> Dict[str, Any]:
    """
    Wrap executor-specific receipts into a common shape.
    """
    base = {
        "normalized": True,
        "ok": bool(ok),
        "venue": venue,
        "symbol": symbol,
    }
    if not isinstance(raw, dict):
        raw = {"raw": raw}
    return {**base, **raw}


# ---------- Command pull/shape ----------

def _shape_intent_from_row(row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Translate a /api/commands/pull row into a normalized intent dict.

    The Bus returns rows shaped like:
        {"id","venue","symbol","side","amount_usd","amount_base","note","mode","agent_id","cmd_id"}

    For backward compatibility we also accept a "payload" sub-dict.
    """
    if not isinstance(row, dict):
        return None

    cid = row.get("id") or row.get("cmd_id")
    if not cid:
        return None

    payload = row.get("payload") or {}
    if not isinstance(payload, dict):
        payload = {}

    def pick(key: str, default: Any = None) -> Any:
        # Prefer flattened row, fall back to nested payload if present.
        return row.get(key, payload.get(key, default))

    venue  = (pick("venue") or "").upper()
    symbol = pick("symbol") or pick("pair") or ""
    side   = (pick("side") or "BUY").upper()

    amount_usd = pick("amount_usd")
    if amount_usd is None:
        amount_usd = pick("amount") or pick("amount_base")
    try:
        amount_usd_f = float(amount_usd) if amount_usd is not None else 0.0
    except Exception:
        amount_usd_f = 0.0

    mode = (pick("mode") or EDGE_MODE).lower()
    note = pick("note") or ""

    intent: Dict[str, Any] = {
        "id": int(cid),
        "cmd_id": int(cid),
        "venue": venue,
        "symbol": symbol,
        "side": side,
        "amount_usd": amount_usd_f,
        "mode": mode,
        "note": note,
        "raw": row,
    }
    # mirror agent_id if present
    agent_id = pick("agent_id")
    if agent_id:
        intent["agent_id"] = agent_id
    return intent


def _poll_commands() -> List[Dict[str, Any]]:
    """
    Pull one or more commands from the bus and normalize them.

    Returns a list of intent dicts. Any malformed commands are ACKed
    as 'held' with an error reason so they don't poison the queue.
    """
    try:
        raw = bus_pull(lease_limit=1)
    except Exception as e:
        log.error("pull error: %s", e)
        return []

    if raw is None:
        return []

    # /api/commands/pull returns a bare list; tolerate wrapper dicts too.
    if isinstance(raw, dict):
        cmds = raw.get("commands") or raw.get("rows") or raw.get("data") or []
    else:
        cmds = raw

    if not isinstance(cmds, list):
        log.warning("unexpected pull payload: %r", raw)
        return []

    intents: List[Dict[str, Any]] = []

    for row in cmds:
        intent = _shape_intent_from_row(row)
        if not intent:
            # Row without ID; nothing we can do.
            continue

        cid    = intent["id"]
        venue  = intent.get("venue") or ""
        symbol = intent.get("symbol") or ""

        if not venue or not symbol:
            # Legacy / malformed rows (e.g. from early experiments).
            msg = f"malformed command id={cid} venue={venue!r} symbol={symbol!r}"
            log.error(msg)
            try:
                bus_ack(
                    cid,
                    ok=False,
                    receipt={
                        "normalized": True,
                        "ok": False,
                        "status": "held",
                        "error": "missing venue/symbol",
                        "detail": msg,
                        "raw_command": row,
                    },
                    status="held",
                )
            except Exception as e:
                log.error("failed to ack malformed command %s: %s", cid, e)
            continue

        intents.append(intent)

    return intents


# ---------- Core execution ----------

def execute_intent(intent: Dict[str, Any]) -> Dict[str, Any]:
    venue  = (intent.get("venue") or "").upper()
    symbol = intent.get("symbol") or intent.get("pair") or ""
    mode   = (intent.get("mode") or EDGE_MODE).lower()

    # Global holds / dry-run routing.
    if EDGE_HOLD:
        return _simulate_receipt(intent, reason="EDGE_HOLD")
    if EDGE_MODE != "live" or mode != "live":
        return _simulate_receipt(intent, reason=f"mode={EDGE_MODE}, intent_mode={mode}")

    exec_fn = import_exec_for_venue(venue)
    raw_res = exec_fn(intent)

    if isinstance(raw_res, dict) and raw_res.get("normalized"):
        return raw_res
    return _normalize_receipt(True, venue, symbol, raw_res)


def main() -> None:
    log.info("online — mode=%s hold=%s base=%s agent=%s", EDGE_MODE, EDGE_HOLD, BASE_URL, AGENT_ID)

    # Start telemetry pusher thread (non-fatal if it fails)
    try:
        start_balance_pusher()
    except Exception as e:
            log.error("failed to start balance pusher: %s", e)

    while True:
        intents = _poll_commands()
        if not intents:
            time.sleep(POLL_SECS)
            continue

        for intent in intents:
            cid    = intent["id"]
            venue  = intent.get("venue") or ""
            symbol = intent.get("symbol") or ""
            log.info("exec cmd=%s agent=%s venue=%s symbol=%s", cid, AGENT_ID, venue, symbol)

            try:
                res = execute_intent(intent)
                ok = True
            except Exception as e:
                log.exception("executor error : %s", e)
                ok = False
                res = {
                    "normalized": True,
                    "ok": False,
                    "status": "error",
                    "error": str(e),
                    "raw_intent": intent,
                }

            try:
                bus_ack(
                    cid,
                    ok=ok,
                    receipt=res if isinstance(res, dict) else {
                        "normalized": True,
                        "ok": False,
                        "status": "error",
                        "error": "non_dict_receipt",
                    },
                    status=None if ok else "error",
                )
            except Exception as e:
                log.exception("ack failed for cmd=%s : %s", cid, e)

        # Slight delay between batches
        time.sleep(POLL_SECS)


if __name__ == "__main__":
    main()
