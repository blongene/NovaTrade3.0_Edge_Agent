#!/usr/bin/env python3
"""edge_bus_poller.py — NovaTrade Edge → Bus poll/exec/ack loop

This is the *safe*, production-shaped poller:
- No syntax errors
- Uses edge_bus_client (signed) with retries
- Idempotent via receipts.jsonl (won't double-execute a command already ACKed ok)
- Minimal logs
- Honors EDGE_HOLD and EDGE_MODE

Env (Edge):
  BUS_BASE_URL or CLOUD_BASE_URL or BASE_URL  (preferred: BUS_BASE_URL)
  EDGE_SECRET (must match Bus OUTBOX_SECRET)
  AGENT_ID (or EDGE_AGENT_ID)
  EDGE_MODE=live|dryrun|dry
  EDGE_HOLD=true|false
  EDGE_PULL_LIMIT (default 3)
  PULL_PERIOD_SECONDS (default 10)

Execution:
  python -m edge_bus_poller
"""

from __future__ import annotations

import json
import os
import time
import traceback
from typing import Any, Dict, List

from edge_bus_client import pull, ack


def _pick_env(*keys: str, default: str = "") -> str:
    for k in keys:
        v = (os.getenv(k) or "").strip()
        if v:
            return v
    return default


AGENT_ID = _pick_env("AGENT_ID", "EDGE_AGENT_ID", default="edge-primary")
EDGE_MODE = _pick_env("EDGE_MODE", default="dry").lower()
EDGE_HOLD = _pick_env("EDGE_HOLD", default="false").lower() in {"1", "true", "yes"}

PULL_PERIOD = int(_pick_env("PULL_PERIOD_SECONDS", default="10"))
MAX_PULL = int(_pick_env("EDGE_PULL_LIMIT", default="3"))
RECEIPTS_PATH = _pick_env("RECEIPTS_PATH", default="./receipts.jsonl")


def _append_receipt(line: Dict[str, Any]) -> None:
    try:
        with open(RECEIPTS_PATH, "a", encoding="utf-8") as f:
            f.write(json.dumps(line, ensure_ascii=False) + "\n")
    except Exception:
        # never crash loop because disk is weird
        pass


def _seen_ok(command_id: str) -> bool:
    try:
        with open(RECEIPTS_PATH, "r", encoding="utf-8") as f:
            for ln in f:
                try:
                    j = json.loads(ln)
                    if str(j.get("command_id")) == str(command_id) and j.get("status") == "ok":
                        return True
                except Exception:
                    continue
    except FileNotFoundError:
        return False
    except Exception:
        return False
    return False


def _ack_ok(cmd_id: str, receipt: Dict[str, Any]) -> None:
    res = ack(AGENT_ID, cmd_id, True, receipt)
    _append_receipt({"ts": int(time.time()), "command_id": str(cmd_id), "status": "ok", "ack": res})


def _ack_err(cmd_id: str, err: str, extras: Dict[str, Any] | None = None) -> None:
    payload: Dict[str, Any] = {"error": err}
    if extras:
        payload.update(extras)
    try:
        res = ack(AGENT_ID, cmd_id, False, payload)
        _append_receipt({"ts": int(time.time()), "command_id": str(cmd_id), "status": "err", "ack": res, "error": err})
    except Exception:
        # don't spin/log too much
        _append_receipt({"ts": int(time.time()), "command_id": str(cmd_id), "status": "err", "error": err})


def _simulate_receipt(cmd: Dict[str, Any], reason: str) -> Dict[str, Any]:
    intent = cmd.get("intent") or {}
    venue = (intent.get("venue") or "").upper()
    symbol = (intent.get("symbol") or "").upper()
    side = (intent.get("side") or "").lower()
    amount = float(intent.get("amount") or 0.0)

    return {
        "normalized": {
            "receipt_id": f"{AGENT_ID}:{cmd.get('id')}",
            "venue": venue,
            "symbol": symbol,
            "side": side,
            "executed_qty": amount,
            "avg_price": 0.0,
            "status": "SIMULATED",
            "reason": reason,
        }
    }


def _execute(cmd: Dict[str, Any]) -> None:
    cid = str(cmd.get("id"))
    if not cid:
        return

    # idempotency: if we already ACKed ok in the local receipts ledger, skip.
    if _seen_ok(cid):
        return

    # Holds + dry modes are *SIMULATED* and still ACK ok (so the command leaves the outbox)
    if EDGE_HOLD:
        _ack_ok(cid, _simulate_receipt(cmd, reason="EDGE_HOLD"))
        return

    mode = (cmd.get("intent") or {}).get("mode")
    mode = (str(mode).lower() if mode else EDGE_MODE)
    if mode not in {"live", "dry", "dryrun"}:
        mode = EDGE_MODE

    if EDGE_MODE != "live" or mode != "live":
        _ack_ok(cid, _simulate_receipt(cmd, reason=f"mode={EDGE_MODE}, intent_mode={mode}"))
        return

    # Live execution: hand off to your existing router/executors.
    # We keep this robust: if anything fails, ACK error once.
    try:
        # Prefer the existing policy-aware executor if present.
        try:
            from edge_policy_aware_executor import execute_intent as _exec
        except Exception:
            _exec = None

        if _exec is None:
            raise RuntimeError("No executor available (edge_policy_aware_executor.execute_intent missing)")

        intent = cmd.get("intent") or {}
        receipt = _exec(intent)

        # Expect a normalized receipt dict; if executor returns plain dict, wrap softly.
        if isinstance(receipt, dict) and ("normalized" in receipt):
            norm = receipt
        else:
            norm = {"normalized": receipt or {"status": "FILLED"}}

        _ack_ok(cid, norm)

    except Exception as e:
        tb = "".join(traceback.format_exception_only(type(e), e)).strip()
        _ack_err(cid, f"execute_failed: {tb}")


def poll_once() -> int:
    try:
        res = pull(AGENT_ID, MAX_PULL)
    except Exception as e:
        # edge_bus_client already rate-limits transient logs; keep quiet here.
        return 0

    cmds: List[Dict[str, Any]] = (res or {}).get("commands") or []
    if not cmds:
        return 0

    for c in cmds:
        _execute(c)

    return len(cmds)


def run_forever() -> None:
    # One clean startup line
    try:
        base = _pick_env("BASE_URL", "BUS_BASE_URL", "CLOUD_BASE_URL", "PUBLIC_BASE_URL", default="")
    except Exception:
        base = ""

    print(f"[edge] bus poller online — agent={AGENT_ID} mode={EDGE_MODE} hold={EDGE_HOLD} base={base}")

    while True:
        poll_once()
        time.sleep(PULL_PERIOD)


def start_bus_poller():
    # Back-compat helper if edge_agent imports this
    import threading

    t = threading.Thread(target=run_forever, name="bus-poller", daemon=True)
    t.start()
    return t


if __name__ == "__main__":
    run_forever()
