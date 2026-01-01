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

# Phase 24B idempotency ledger (kept; not stripped)
from edge_idempotency import claim as _idem_claim, mark_done as _idem_done

from telemetry_sender import start_balance_pusher  # existing helper
from telemetry_sender import _collect_balances


# ---------- Config ----------

BASE_URL = (
    (os.getenv("CLOUD_BASE_URL") or os.getenv("BASE_URL") or "").rstrip("/")
    or "https://novatrade3-0.onrender.com"
)

# Secret tolerance: prefer EDGE_SECRET; fall back if needed
EDGE_SECRET = os.getenv("EDGE_SECRET", "") or ""
OUTBOX_SECRET = os.getenv("OUTBOX_SECRET", "") or ""
TELEMETRY_SECRET = os.getenv("TELEMETRY_SECRET", "") or ""

AGENT_ID = os.getenv("AGENT_ID", "edge-primary")

EDGE_MODE = os.getenv("EDGE_MODE", "live").lower()   # "live" or "dryrun"
EDGE_HOLD = os.getenv("EDGE_HOLD", "false").lower() == "true"

POLL_SECS = float(os.getenv("EDGE_POLL_SECS", "5"))
TIMEOUT = float(os.getenv("EDGE_HTTP_TIMEOUT", "15"))

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


def _pick_secret() -> str:
    """
    Prefer EDGE_SECRET, then OUTBOX_SECRET, then TELEMETRY_SECRET.
    """
    return (EDGE_SECRET or OUTBOX_SECRET or TELEMETRY_SECRET or "").strip()


def _canonical_json_bytes(body: Dict[str, Any]) -> bytes:
    """
    Canonical JSON encoding used for signing and as the exact request body.
    """
    return json.dumps(
        body,
        separators=(",", ":"),  # no spaces
        sort_keys=True,         # stable field order
    ).encode("utf-8")


def _signed_headers(sig: str) -> Dict[str, str]:
    """
    Put the same signature under multiple header names for Bus compatibility.
    """
    headers = {"Content-Type": "application/json"}
    if not sig:
        return headers

    # Canonical / modern
    headers["X-OUTBOX-SIGN"] = sig

    # Legacy/compat variants (HTTP headers are case-insensitive, but we include common spellings)
    headers["X-NT-Sig"] = sig
    headers["X-TELEMETRY-SIGN"] = sig
    headers["X-Nova-Signature"] = sig
    headers["X-NOVA-SIGNATURE"] = sig
    headers["X-NOVA-SIGN"] = sig

    return headers


def _http_snip(text: str, n: int = 220) -> str:
    try:
        s = (text or "").strip().replace("\n", " ")
        if len(s) > n:
            return s[:n] + "…"
        return s
    except Exception:
        return ""


def _post_json(path: str, body: Dict[str, Any]) -> Any:
    """
    POST signed JSON to the bus.

    We always:
      - JSON-serialize with sort_keys=True and compact separators
      - HMAC that canonical JSON with EDGE/OUTBOX/TELEMETRY secret tolerance
      - Send it as the request body with multiple signature headers

    Returns:
      - Parsed JSON (dict/list) when possible
      - None when response is non-json but 2xx
    Raises:
      - requests.HTTPError on HTTP >= 400 (with status + snippet)
    """
    secret = _pick_secret()
    raw_sorted = _canonical_json_bytes(body)
    sig = _hmac_sha256_hex(secret, raw_sorted) if secret else ""

    # Debug line so we can see exactly what the Edge is sending
    log.info(
        "hmac_debug path=%s body=%s sig=%s",
        path,
        raw_sorted.decode("utf-8"),
        sig or "<empty>",
    )

    url = f"{BASE_URL}{path}"
    headers = _signed_headers(sig)

    r = requests.post(url, data=raw_sorted, headers=headers, timeout=TIMEOUT)

    # Always log the HTTP result (this is the missing visibility that will unblock the leasing issue fast)
    log.info("http_debug path=%s status=%s snip=%s", path, r.status_code, _http_snip(r.text))

    if r.status_code >= 400:
        raise requests.HTTPError(f"{r.status_code} {_http_snip(r.text, 800)}")

    try:
        return r.json()
    except Exception:
        return None


def bus_pull(lease_limit: int = 1) -> Any:
    """
    Pull NEW commands from /api/commands/pull.

    IMPORTANT: We include multiple agent keys because different Bus builds filter differently.
    """
    aid = str(AGENT_ID).strip()
    lim = int(lease_limit)

    body = {
        # Compatibility agent keys:
        "agent_id": aid,
        "agent": aid,
        "agent_target": aid,
        "agentId": aid,
        "agent_name": aid,

        # Paging keys (compat):
        "limit": lim,
        "max_items": lim,
        "n": lim,

        "ts": int(time.time()),
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
    aid = str(AGENT_ID).strip()

    body: Dict[str, Any] = {
        "id": int(cmd_id),
        # Keep agent_id canonical, but also include variants for mixed Bus builds
        "agent_id": aid,
        "agent": aid,
        "agent_target": aid,
        "agentId": aid,
        "agent_name": aid,

        "status": state,
        "receipt": receipt,
        "ts": int(time.time()),
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
        "KRAKEN": "executors.kraken_executor",
        "BINANCEUS": "executors.binance_us_executor",
        # Coinbase uses the advanced executor variant
        "COINBASE": "executors.coinbase_advanced_executor",
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
    venue = (intent.get("venue") or "").upper()
    symbol = intent.get("symbol") or intent.get("pair") or ""
    side = (intent.get("side") or "BUY").upper()
    amt = float(intent.get("amount_usd") or intent.get("amount") or 0)

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

    Supported shapes:

    1) Flat (legacy):
        {
          "id": 123,
          "venue": "BINANCEUS",
          "symbol": "OCEANUSDT",
          "side": "BUY",
          "amount_usd": 50,
          "mode": "live",
          "note": "...",
          "agent_id": "edge-primary",
        }

    2) Nested (current Bus outbox):
        {
          "id": 18,
          "intent": {
            "agent_id": "edge-primary",
            "venue": "KRAKEN",
            "payload": {
              "token": "OCEAN",
              "quote": "USDT",
              "amount_usd": 4.52,
              "type": "manual_rebuy",
              "intent_id": "manual_rebuy:OCEAN:1763694168"
            }
          },
          "status": "held"
        }

    We support both by:
      - Looking at row, then intent, then payload
      - Deriving symbol from token+quote if needed
    """
    if not isinstance(row, dict):
        return None

    intent_meta = row.get("intent")
    if not isinstance(intent_meta, dict):
        intent_meta = {}

    payload = row.get("payload")
    if not isinstance(payload, dict):
        inner = intent_meta.get("payload")
        payload = inner if isinstance(inner, dict) else {}

    def pick(key: str, default: Any = None) -> Any:
        for src in (row, intent_meta, payload):
            if isinstance(src, dict) and key in src:
                val = src.get(key)
                if val not in (None, ""):
                    return val
        return default

    cid = pick("id") or pick("cmd_id")
    if not cid:
        return None

    venue = (pick("venue") or "").upper()

    symbol = pick("symbol") or pick("pair") or ""
    base = pick("token") or pick("base")
    quote = pick("quote") or pick("quote_asset")

    if not symbol and base and quote:
        symbol = f"{str(base).upper()}/{str(quote).upper()}"

    side = (pick("side") or "BUY").upper()

    amount_usd = pick("amount_usd")
    if amount_usd is None:
        amount_usd = pick("amount") or pick("amount_base") or pick("amount_quote")

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

    if base:
        base_uc = str(base).upper()
        intent.setdefault("token", base_uc)
        intent.setdefault("base", base_uc)
    if quote:
        quote_uc = str(quote).upper()
        intent.setdefault("quote", quote_uc)

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
            continue

        cid = intent["id"]
        venue = intent.get("venue") or ""
        symbol = intent.get("symbol") or ""

        if not venue or not symbol:
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
    venue = (intent.get("venue") or "").upper()
    symbol = intent.get("symbol") or intent.get("pair") or ""
    mode = (intent.get("mode") or EDGE_MODE).lower()

    if EDGE_HOLD:
        return _simulate_receipt(intent, reason="EDGE_HOLD")
    if EDGE_MODE != "live" or mode != "live":
        return _simulate_receipt(intent, reason=f"mode={EDGE_MODE}, intent_mode={mode}")

    exec_fn = import_exec_for_venue(venue)
    raw_res = exec_fn(intent)

    if isinstance(raw_res, dict) and raw_res.get("normalized"):
        return raw_res

    ok_flag = True
    if isinstance(raw_res, dict):
        if "ok" in raw_res:
            ok_flag = bool(raw_res.get("ok"))
        else:
            status = str(raw_res.get("status", "")).lower()
            ok_flag = not status or status == "ok"

    return _normalize_receipt(ok_flag, venue, symbol, raw_res)


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
            cid = intent["id"]
            venue = intent.get("venue") or ""
            symbol = intent.get("symbol") or ""
            log.info("exec cmd=%s agent=%s venue=%s symbol=%s", cid, AGENT_ID, venue, symbol)

            try:
                res = execute_intent(intent)
                ok = bool(isinstance(res, dict) and res.get("ok", True))
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

        time.sleep(POLL_SECS)


if __name__ == "__main__":
    main()
