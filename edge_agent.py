#!/usr/bin/env python3
# edge_agent.py — minimal, robust Edge that leases, executes, and ACKs

import os, time, json, hmac, hashlib, logging, importlib, inspect
from typing import Any, Dict, Optional

import requests

# ---------- Config / ENV ----------
BASE_URL     = os.getenv("BASE_URL", "").rstrip("/")
AGENT_ID     = os.getenv("AGENT_ID", "edge-primary")
EDGE_SECRET  = os.getenv("EDGE_SECRET", "")
EDGE_MODE    = os.getenv("EDGE_MODE", "live")  # live|dry
EDGE_HOLD    = os.getenv("EDGE_HOLD", "false").lower() == "true"
POLL_SECS    = int(os.getenv("EDGE_POLL_SECS", "60"))
TIMEOUT      = int(os.getenv("EDGE_HTTP_TIMEOUT", "15"))

logging.basicConfig(
    level=logging.INFO,
    format="[edge] %(levelname)s %(asctime)s %(message)s",
)
log = logging.getLogger("edge")

if not BASE_URL or not EDGE_SECRET:
    raise SystemExit("BASE_URL and EDGE_SECRET must be set")

# ---------- HTTP helpers (HMAC) ----------
def _hmac_sha256(secret: str, raw: str) -> str:
    return hmac.new(secret.encode(), raw.encode(), hashlib.sha256).hexdigest()

def _post_json(path: str, body: Dict[str, Any]) -> Dict[str, Any]:
    raw = json.dumps(body, separators=(",", ":"), sort_keys=True)
    sig = _hmac_sha256(EDGE_SECRET, raw)
    r = requests.post(
        f"{BASE_URL}{path}",
        data=raw,
        headers={
            "Content-Type": "application/json",
            "X-Nova-Signature": sig,
        },
        timeout=TIMEOUT,
    )
    if r.status_code >= 400:
        raise requests.HTTPError(f"{r.status_code} {r.text}")
    return r.json()

def bus_pull(lease_limit: int = 1) -> Dict[str, Any]:
    body = {"agent_id": AGENT_ID, "limit": int(lease_limit), "ts": int(time.time())}
    return _post_json("/api/commands/pull", body)

def bus_ack(cmd_id: int, ok: bool, receipt: Dict[str, Any]) -> Dict[str, Any]:
    body = {"agent_id": AGENT_ID, "cmd_id": int(cmd_id), "ok": bool(ok), "receipt": receipt}
    return _post_json("/api/commands/ack", body)

# ---------- Venue resolver ----------
def import_exec_for_venue(venue: str):
    """
    Import the right executor module for a venue and return its execute_market_order function.
    Coinbase uses the advanced executor.
    """
    module_map = {
        "KRAKEN":    "executors.kraken_executor",
        "BINANCEUS": "executors.binance_us_executor",
        "COINBASE":  "executors.coinbase_advanced_executor",  # important!
    }
    mod_name = module_map.get(venue.upper())
    if not mod_name:
        raise ImportError(f"Unsupported venue: {venue}")

    mod = importlib.import_module(mod_name)
    fn  = getattr(mod, "execute_market_order", None)
    if not callable(fn):
        raise AttributeError(f"{mod_name}.execute_market_order missing or not callable")
    return fn

# ---------- Signature-aware executor adapter ----------
def _call_exec(exec_fn, intent: Dict[str, Any]) -> Dict[str, Any]:
    """
    Call executor regardless of its signature:
      - ()                       -> call without args
      - (intent)                 -> pass the whole intent
      - (**kwargs)               -> map common fields and pass kwargs
    """
    try:
        sig = inspect.signature(exec_fn)
    except Exception:
        # can’t inspect: best bet is the historical one-arg pattern
        return exec_fn(intent)

    params = list(sig.parameters.values())
    required = [
        p for p in params
        if p.kind in (inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD)
        and p.default is inspect._empty
    ]

    # Case 1: no required args -> call with no arguments
    if len(required) == 0:
        return exec_fn()

    # Case 2: one required arg -> treat it as (intent)
    if len(required) == 1 and required[0].kind in (
        inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD
    ):
        return exec_fn(intent)

    # Case 3: kwargs mapping
    names = set(sig.parameters.keys())
    kw: Dict[str, Any] = {}

    # Common fields executors might want
    for key in ("symbol", "side", "venue", "amount", "quote_amount", "quote_amount_usd"):
        if key in names and key in intent:
            kw[key] = intent[key]

    # Some executors prefer 'pair' instead of 'symbol'
    if "pair" in names:
        if "pair" in intent:
            kw["pair"] = intent["pair"]
        elif "symbol" in intent:
            kw["pair"] = intent["symbol"]  # let executor normalize internally

    if kw:
        return exec_fn(**kw)

    # Last resort
    return exec_fn(intent)

# ---------- Normalization helpers ----------
def _normalize_receipt(ok: bool, venue: str, symbol: str, raw: Dict[str, Any]) -> Dict[str, Any]:
    """
    Standardize what we ACK back to the Bus.
    Executors may already return in this shape; if so, we pass-through.
    """
    if raw and "normalized" in raw:  # executor already shaped it
        return raw

    rid = raw.get("receipt_id") or raw.get("id") or f"edge-{int(time.time())}"
    status = raw.get("status", "ok" if ok else "error")
    return {
        "normalized": {
            "receipt_id": str(rid),
            "venue": venue,
            "symbol": symbol,
            "status": status,
            # optional pass-throughs if present
            **({k: raw[k] for k in ("executed_qty", "avg_price", "fee", "fee_asset") if k in raw}),
        }
    }

def _simulate_receipt(intent: Dict[str, Any]) -> Dict[str, Any]:
    """Used when EDGE_MODE=dry or EDGE_HOLD=true."""
    rid = f"sim-{int(time.time())}"
    return {
        "normalized": {
            "receipt_id": rid,
            "venue": intent.get("venue", ""),
            "symbol": intent.get("symbol", ""),
            "status": "simulated",
        }
    }

# ---------- Core execution ----------
def execute_intent(intent: Dict[str, Any]) -> Dict[str, Any]:
    venue  = intent.get("venue", "").upper()
    symbol = intent.get("symbol", "")

    if EDGE_HOLD:
        return _simulate_receipt(intent)
    if EDGE_MODE != "live":
        return _simulate_receipt(intent)

    try:
        exec_fn = import_exec_for_venue(venue)
    except Exception as e:
        raise RuntimeError(f"EXECUTOR_MISSING: {e}")

    # Run the executor with signature tolerance
    res = _call_exec(exec_fn, intent)

    # If executor already returns normalized, pass-through; else wrap
    if isinstance(res, dict) and "normalized" in res:
        return res
    return _normalize_receipt(True, venue, symbol, res if isinstance(res, dict) else {})

# ---------- Main poll loop ----------
def main():
    log.info(f"online — mode={EDGE_MODE} hold={EDGE_HOLD} base={BASE_URL} agent={AGENT_ID}")
    while True:
        try:
            lease = bus_pull(1)
        except Exception as e:
            log.error(f"pull error: {e}")
            time.sleep(POLL_SECS)
            continue

        cmds = lease.get("commands") or []
        if not cmds:
            time.sleep(POLL_SECS)
            continue

        for cmd in cmds:
            cid = cmd.get("id")

            # 🔹 Unwrap envelope from Bus → Edge
            intent = cmd.get("intent")
            if not intent:
                envelope = cmd.get("payload") or {}
                if isinstance(envelope, dict):
                    # New-style: {agent_id, type, payload={...}} or {agent_id, type, intent={...}}
                    intent = (
                        envelope.get("intent")
                        or envelope.get("payload")
                        or envelope
                    )
                else:
                    intent = {}

            try:
                res = execute_intent(intent or {})
                log.info(f"ack cmd={cid} ok=True")
                bus_ack(cid, True, res)
            except Exception as e:
                # best-effort failure receipt
                venue  = (intent or {}).get("venue", "")
                symbol = (intent or {}).get("symbol", "")
                log.error(f"executor error {venue}: {e}", exc_info=True)
                fail = {
                    "normalized": {
                        "receipt_id": f"err-{int(time.time())}",
                        "venue": venue,
                        "symbol": symbol,
                        "status": "error",
                        "error": str(e),
                    }
                }
                bus_ack(cid, False, fail)

        # Immediately loop; the Bus controls lease cadence
from telemetry_sender import start_balance_pusher
start_balance_pusher()

if __name__ == "__main__":
    main()
