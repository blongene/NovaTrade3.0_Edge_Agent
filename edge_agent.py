#!/usr/bin/env python3
"""
Edge Agent — robust, import-safe executor loader.

- Lazily loads venue executors so missing modules/functions never crash boot.
- Accepts either `execute_market_order(intent)` or `execute(intent)` in the executor.
- Honors EDGE_MODE (dry/live) and VENUES_ENABLED allowlist.
- Pulls one command at a time, executes (or simulates), and ACKs with a normalized receipt.
"""

from __future__ import annotations
import os, json, time, hmac, hashlib, importlib, traceback
from typing import Any, Callable, Dict, Optional

# -------------------- Env --------------------
BASE_URL       = os.getenv("BASE_URL", "").rstrip("/")
EDGE_SECRET    = os.getenv("EDGE_SECRET", "")
AGENT_ID       = os.getenv("AGENT_ID", "edge-primary")
EDGE_MODE      = (os.getenv("EDGE_MODE", "dry") or "dry").lower()  # dry | live
VENUES_ENABLED = set([v.strip().upper() for v in os.getenv("VENUES_ENABLED", "COINBASE,KRAKEN").split(",") if v.strip()])
POLL_SECS      = int(os.getenv("EDGE_POLL_SECS", "60"))

assert BASE_URL and EDGE_SECRET, "BASE_URL and EDGE_SECRET must be set"

def _log(level: str, msg: str):
    print(f"[edge] {level.upper()} {time.strftime('%Y-%m-%d %H:%M:%S')} {msg}", flush=True)

# -------------------- Signing helpers --------------------
def _hmac_sig(raw: bytes) -> str:
    return hmac.new(EDGE_SECRET.encode(), raw, hashlib.sha256).hexdigest()

def _post(path: str, body: Dict[str, Any], timeout: int = 15) -> Dict[str, Any]:
    import requests
    raw = json.dumps(body, separators=(",",":"), sort_keys=True).encode()
    sig = _hmac_sig(raw)
    r = requests.post(f"{BASE_URL}{path}",
                      data=raw,
                      headers={"Content-Type":"application/json",
                               "X-Nova-Signature": sig},
                      timeout=timeout)
    r.raise_for_status()
    return r.json()

def pull(agent_id: str, limit: int = 1) -> Dict[str, Any]:
    body = {"agent_id": agent_id, "limit": int(limit), "ts": int(time.time())}
    return _post("/api/commands/pull", body)

def ack(agent_id: str, cmd_id: int, ok: bool, receipt: Dict[str, Any]) -> Dict[str, Any]:
    body = {"agent_id": agent_id, "cmd_id": int(cmd_id), "ok": bool(ok), "receipt": receipt}
    return _post("/api/commands/ack", body)

# -------------------- Executor Registry (lazy & tolerant) --------------------
# Map venue -> (module_name, function_name_preference_list)
_EXEC_SPECS = {
    "COINBASE":   ("executors.coinbase_executor",   ["execute_market_order", "execute"]),
    "KRAKEN":     ("executors.kraken_executor",     ["execute_market_order", "execute"]),
    "BINANCEUS":  ("executors.binance_executor",    ["execute_market_order", "execute"]),  # your file name
    "BINANCE_US": ("executors.binance_executor",    ["execute_market_order", "execute"]),  # alias tolerated
}

# Cache: venue -> callable or None
_EXEC_CACHE: Dict[str, Optional[Callable[[Dict[str, Any]], Dict[str, Any]]]] = {}

def _load_executor(venue: str) -> Optional[Callable[[Dict[str, Any]], Dict[str, Any]]]:
    v = venue.upper()
    if v in _EXEC_CACHE:
        return _EXEC_CACHE[v]

    spec = _EXEC_SPECS.get(v)
    if not spec:
        _EXEC_CACHE[v] = None
        return None

    module_name, func_names = spec
    try:
        mod = importlib.import_module(module_name)
    except Exception as e:
        _log("warn", f"executor import failed for {v} ({module_name}): {e}")
        _EXEC_CACHE[v] = None
        return None

    func = None
    for fn in func_names:
        if hasattr(mod, fn):
            func = getattr(mod, fn)
            break

    if not func or not callable(func):
        _log("warn", f"executor function missing for {v} in {module_name} (tried {func_names})")
        _EXEC_CACHE[v] = None
        return None

    _EXEC_CACHE[v] = func
    return func

# -------------------- Execution --------------------
def _simulate_receipt(intent: Dict[str, Any]) -> Dict[str, Any]:
    # Good enough for Bus plumbing + Sheets logging in DRY mode
    symbol   = intent.get("symbol", "ETH/USDC")
    side     = intent.get("side", "BUY")
    venue    = intent.get("venue", "COINBASE").upper()
    amount   = float(intent.get("amount", 25))
    price    = float(intent.get("price_usd", 1.0))
    rid      = f"sim-{int(time.time())}-{venue}-{symbol}-{side}"

    return {
        "status": "ok",
        "normalized": {
            "receipt_id":  rid,
            "venue":       venue,
            "symbol":      symbol,
            "side":        side,
            "executed_qty": amount,
            "avg_price":   price,
            "quote_spent": round(amount * price, 8),
            "fee":         0.0,
            "fee_asset":   symbol.split("/")[-1] if "/" in symbol else "USDC",
            "order_id":    rid,
            "txid":        rid,
            "status":      "filled",
        }
    }

def execute_intent(intent: Dict[str, Any]) -> Dict[str, Any]:
    venue = str(intent.get("venue", "")).upper()
    if not venue:
        return {"status":"error","message":"missing venue"}

    if VENUES_ENABLED and venue not in VENUES_ENABLED:
        return {"status":"skipped","reason":"VENUE_DISABLED","venue":venue}

    if EDGE_MODE == "dry":
        return _simulate_receipt(intent)

    exec_fn = _load_executor(venue)
    if not exec_fn:
        return {"status":"skipped","reason":"EXECUTOR_MISSING","venue":venue}

    try:
        res = exec_fn(intent)  # executor returns normalized dict as contract
        # normalize minimal contract expectations
        if not isinstance(res, dict):
            return {"status":"error","message":"executor returned non-dict"}
        if res.get("status") not in {"ok","filled","success"}:
            # pass-through any error info
            return res
        # force "ok"
        res["status"] = "ok"
        return res
    except Exception as e:
        _log("error", f"executor error {venue}: {e}\n{traceback.format_exc()}")
        return {"status":"error","message":str(e)}

# -------------------- Main Loop --------------------
def main() -> None:
    _log("info", f"online — mode={EDGE_MODE} hold={os.getenv('EDGE_HOLD','false')} base={BASE_URL} agent={AGENT_ID}")
    while True:
        try:
            r = pull(AGENT_ID, 1)
            cmds = r.get("commands") or []
            if not cmds:
                time.sleep(POLL_SECS)
                continue

            for c in cmds:
                cid    = c.get("id")
                intent = c.get("intent") or {}

                res = execute_intent(intent)

                ok = bool(res.get("status") == "ok")
                # Build a compact ack payload with normalized receipt if present
                receipt = {"normalized": res.get("normalized") or {}}
                if not ok:
                    # include reason/message in the ack for observability
                    reason = res.get("reason") or res.get("message") or res.get("status") or "error"
                    receipt["error"] = reason

                ack(AGENT_ID, int(cid), ok, receipt)
                _log("info", f"ack cmd={cid} ok={ok} reason={res.get('reason') or res.get('message') or res.get('status')}")

        except Exception as e:
            _log("error", f"loop error: {e}\n{traceback.format_exc()}")
            time.sleep(POLL_SECS)

if __name__ == "__main__":
    main()
