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


def _dryrun_receipt(intent: Dict[str, Any], *, reason: str = "dryrun") -> Dict[str, Any]:
    """Return an ok receipt for dryrun execution without hitting any venue."""
    venue = (intent.get("venue") or "").upper()
    symbol = intent.get("symbol") or intent.get("pair") or ""
    side = (intent.get("side") or "BUY").upper()
    amt_usd = float(intent.get("amount_usd") or intent.get("amount") or 0)
    amt_base = intent.get("amount_base")
    amt_quote = intent.get("amount_quote")
    return {
        "normalized": True,
        "ok": True,
        "status": "dryrun",
        "venue": venue,
        "symbol": symbol,
        "side": side,
        "amount_usd": amt_usd,
        "amount_base": amt_base,
        "amount_quote": amt_quote,
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

    Supports multiple command shapes:
      - Newer: {"id":..., "intent": {"type": "...", "payload": {...}}}
      - Alt:   {"id":..., "command": {"type": "...", "payload": {...}}}
      - Flat:  {"type": "...", "venue": "...", ...}  (legacy)

    Key guarantees:
      - Always sets side (default BUY)
      - Correctly reads nested payload fields (venue/symbol/side/amounts)
      - Preserves amount_base / amount_quote / amount_usd when provided
      - Preserves flags
      - Adds mode="dryrun" when dry_run=True (helps executors)
    """
    if not isinstance(row, dict):
        return None

    # --- Locate the "intent container" (outer) ---
    it = row.get("intent") if isinstance(row.get("intent"), dict) else None
    if not it:
        it = row.get("command") if isinstance(row.get("command"), dict) else None
    if not it:
        it = row  # legacy/flat

    # --- Locate payload (inner) ---
    payload = None
    if isinstance(it, dict) and isinstance(it.get("payload"), dict):
        payload = it["payload"]
    elif isinstance(row.get("payload"), dict):
        payload = row["payload"]

    # --- Meta can live inside payload["meta"] (most common) ---
    meta = None
    if isinstance(payload, dict) and isinstance(payload.get("meta"), dict):
        meta = payload["meta"]
    elif isinstance(it, dict) and isinstance(it.get("meta"), dict):
        meta = it["meta"]
    elif isinstance(row.get("meta"), dict):
        meta = row["meta"]

    # Search order: payload -> intent container -> row
    search_dicts = [d for d in (payload, it, row) if isinstance(d, dict)]

    def pick(*keys: str) -> Any:
        """Pick first non-None value from payload/intent/row for any of the given keys."""
        for k in keys:
            for d in search_dicts:
                if k in d and d[k] is not None:
                    return d[k]
        return None

    def pick_meta(*keys: str) -> Any:
        """Pick first non-None value from meta, else fallback to pick()."""
        if isinstance(meta, dict):
            for k in keys:
                if k in meta and meta[k] is not None:
                    return meta[k]
        return pick(*keys)

    # --- Flags (keep list[str]) ---
    flags_raw = pick("flags", "intent_flags")
    if flags_raw is None:
        flags_raw = pick_meta("flags", "intent_flags")
    if isinstance(flags_raw, (list, tuple)):
        flags = [str(x) for x in flags_raw]
    elif flags_raw:
        flags = [str(flags_raw)]
    else:
        flags = []
    flags_l = [f.lower() for f in flags]

    # --- Type ---
    # Prefer outer "type" (e.g., it["type"] == "order.place")
    # but allow payload/type in legacy shapes
    cmd_type = pick("type", "command_type", "intent_type")
    if isinstance(it, dict) and isinstance(it.get("type"), str):
        cmd_type = it.get("type") or cmd_type
    if not cmd_type:
        cmd_type = "trade"

    # --- Venue + Symbol ---
    venue = (pick("venue") or pick_meta("venue") or "").strip()
    venue_uc = venue.upper()

    base = pick("base", "token") or pick_meta("base", "token")
    quote = pick("quote") or pick_meta("quote")

    symbol = (pick("symbol", "pair") or pick_meta("symbol", "pair") or "").strip()

    # If base/quote exist and symbol missing, synthesize
    if not symbol and base and quote:
        symbol = f"{str(base).upper()}/{str(quote).upper()}"

    # Normalize symbol lightly (don’t over-convert; executors may handle venue formats)
    # If you want: keep "/" form in the bus; executor will map to venue-specific product id.
    if not symbol and isinstance(pick("product_id"), str):
        symbol = str(pick("product_id")).strip()

    # --- Side MUST be defined ---
    side_raw = pick("side") or pick_meta("side") or "BUY"
    try:
        side = str(side_raw).upper().strip()
    except Exception:
        side = "BUY"
    if side not in ("BUY", "SELL"):
        # tolerate HOLD/NOOP/NOTE payloads by defaulting BUY
        side = "BUY"

    # --- Amounts ---
    def _to_f(x: Any) -> float:
        try:
            if x is None:
                return 0.0
            return float(x)
        except Exception:
            return 0.0

    amount_base_f = _to_f(pick("amount_base") or pick_meta("amount_base"))
    amount_quote_f = _to_f(pick("amount_quote") or pick_meta("amount_quote"))

    # amount_usd is treated as quote sizing by some executors (Coinbase executor supports this)
    amount_usd_f = _to_f(pick("amount_usd") or pick_meta("amount_usd"))

    # Legacy single "amount"
    amount_any = pick("amount") or pick_meta("amount")
    if amount_any is not None:
        if "base" in flags_l and amount_base_f == 0.0:
            amount_base_f = _to_f(amount_any)
        else:
            if amount_quote_f == 0.0:
                amount_quote_f = _to_f(amount_any)
            if amount_usd_f == 0.0:
                amount_usd_f = _to_f(amount_any)

    # If amount_quote not set but amount_usd is, mirror it for compatibility
    if amount_quote_f == 0.0 and amount_usd_f > 0.0:
        amount_quote_f = amount_usd_f

    # --- Dryrun / mode ---
    dry_run_val = pick("dry_run")
    if dry_run_val is None:
        dry_run_val = pick_meta("dry_run")
    if dry_run_val is None:
        # infer from mode string
        mode_s = str(pick("mode") or pick_meta("mode") or "").lower()
        dry_run = (mode_s == "dryrun" or mode_s == "dry_run" or mode_s == "preview")
    else:
        dry_run = bool(dry_run_val)

    mode = pick("mode") or pick_meta("mode")
    if not mode:
        mode = "dryrun" if dry_run else "live"

    # --- IDs ---
    cmd_id = row.get("id") or pick("id")  # row id is the command id
    agent_id = pick("agent_id") or pick_meta("agent_id")

    client_order_id = (
        pick("client_order_id", "client_id", "idempotency_key")
        or pick_meta("client_order_id", "client_id", "idempotency_key")
    )
    idempotency_key = (
        pick("idempotency_key", "client_order_id", "client_id")
        or pick_meta("idempotency_key", "client_order_id", "client_id")
    )

    note = pick("note", "reason") or pick_meta("note", "reason") or ""

    intent: Dict[str, Any] = {
        "id": cmd_id,
        "type": str(cmd_type),
        "venue": venue_uc,
        "symbol": symbol,
        "side": side,

        # sizing (compat + preferred)
        "amount_usd": float(amount_usd_f),
        "amount_quote": float(amount_quote_f),
        "amount_base": float(amount_base_f),

        "flags": flags,

        "dry_run": dry_run,
        "mode": "dryrun" if dry_run else str(mode),

        "client_order_id": client_order_id,
        "idempotency_key": idempotency_key,

        "note": note,
        "raw": row,
    }

    # Token fields
    if base:
        base_uc = str(base).upper()
        intent.setdefault("token", base_uc)
        intent.setdefault("base", base_uc)
    if quote:
        intent.setdefault("quote", str(quote).upper())

    if agent_id:
        intent["agent_id"] = agent_id

    # Preserve meta if present (useful for receipts/telemetry)
    if isinstance(meta, dict):
        intent["meta"] = meta

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
    intent_type = (intent.get("type") or "").lower()

    # Informational intents should never call an executor.
    if intent_type in {"note", "noop"}:
        return {
            "normalized": True,
            "ok": True,
            "status": "noop",
            "venue": venue,
            "symbol": symbol,
            "message": f"{intent_type} intent acknowledged",
            "raw_intent": intent,
        }

    # Phase 26D-preview / Alpha dryruns: allow full pipe without placing live orders.
    if intent_type == "order.place" and bool(intent.get("dry_run")):
        return _dryrun_receipt(intent, reason="order.place dry_run")

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
