#!/usr/bin/env python3
"""edge_bus_poller.py — NovaTrade Edge → Bus poll/exec/ack loop (Phase 25C)

Production goals:
- No syntax errors
- Uses edge_bus_client (signed) with retries + auth fallback
- Idempotent via receipts.jsonl (won't double-execute a command already ACKed ok)
- Minimal logs (rate-limited)
- Honors EDGE_HOLD and EDGE_MODE
- Tolerates varying command shapes (cmd id key differences)

Env (Edge):
  BUS_BASE_URL or CLOUD_BASE_URL or BASE_URL  (preferred: BUS_BASE_URL)
  EDGE_SECRET or OUTBOX_SECRET (must match what Bus expects for /api/commands/*)
  AGENT_ID (or EDGE_AGENT_ID)
  EDGE_MODE=live|dryrun|dry
  EDGE_HOLD=true|false
  EDGE_PULL_LIMIT (default 3)
  PULL_PERIOD_SECONDS (default 10)
  RECEIPTS_PATH (default ./receipts.jsonl)

Run:
  python -m edge_bus_poller
"""

from __future__ import annotations

import json
import os
import time
import traceback
from typing import Any, Dict, List, Optional

from edge_bus_client import pull, ack


def _pick_env(*keys: str, default: str = "") -> str:
    for k in keys:
        v = (os.getenv(k) or "").strip()
        if v:
            return v
    return default


def agent_id() -> str:
    """
    Canonical agent id for Edge polling/acking.
    Priority:
      1) AGENT_ID
      2) EDGE_AGENT_ID
      3) fallback "edge"
    """
    v = _pick_env("AGENT_ID", "EDGE_AGENT_ID", default="edge")
    v = str(v).strip()
    return v or "edge"


AGENT_ID = agent_id()
EDGE_MODE = _pick_env("EDGE_MODE", default="dry").lower()
EDGE_HOLD = _pick_env("EDGE_HOLD", default="false").lower() in {"1", "true", "yes"}

PULL_PERIOD = int(_pick_env("PULL_PERIOD_SECONDS", default="10"))
MAX_PULL = int(_pick_env("EDGE_PULL_LIMIT", default="3"))
RECEIPTS_PATH = _pick_env("RECEIPTS_PATH", default="./receipts.jsonl")

_last_info_ts = 0.0
_last_empty_ts = 0.0


def _now() -> int:
    return int(time.time())


def _append_receipt(line: Dict[str, Any]) -> None:
    try:
        with open(RECEIPTS_PATH, "a", encoding="utf-8") as f:
            f.write(json.dumps(line, ensure_ascii=False) + "\n")
    except Exception:
        # Never crash poller due to local disk issues
        pass


def _seen_ok(command_id: str) -> bool:
    """
    Idempotency: if we already ACKed ok for this command id, don't execute again.
    """
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


def _cmd_id(cmd: Dict[str, Any]) -> str:
    """
    Commands may expose id in multiple places depending on bus schema.
    Prefer:
      - cmd["id"]
      - cmd["cmd_id"]
      - cmd["intent"]["cmd_id"]
      - cmd["intent"]["id"]
    """
    for k in ("id", "cmd_id"):
        v = cmd.get(k)
        if v is not None and str(v).strip():
            return str(v).strip()

    intent = cmd.get("intent") or {}
    for k in ("cmd_id", "id"):
        v = intent.get(k)
        if v is not None and str(v).strip():
            return str(v).strip()

    # last resort: stable-ish
    return str(cmd.get("intent_hash") or cmd.get("intentHash") or "").strip() or ""


def _ack_ok(cid: str, receipt: Dict[str, Any]) -> None:
    res = ack(AGENT_ID, cid, True, receipt)
    _append_receipt({"ts": _now(), "command_id": str(cid), "status": "ok", "ack": res})


def _ack_err(cid: str, err: str, extras: Optional[Dict[str, Any]] = None) -> None:
    payload: Dict[str, Any] = {"error": err}
    if extras:
        payload.update(extras)
    try:
        res = ack(AGENT_ID, cid, False, payload)
        _append_receipt({"ts": _now(), "command_id": str(cid), "status": "err", "ack": res, "error": err})
    except Exception:
        _append_receipt({"ts": _now(), "command_id": str(cid), "status": "err", "error": err})


def _simulate_receipt(cmd: Dict[str, Any], reason: str) -> Dict[str, Any]:
    intent = cmd.get("intent") or {}
    # keep it small but structured
    return {
        "normalized": {
            "receipt_id": f"{AGENT_ID}:{_cmd_id(cmd)}",
            "status": "SIMULATED",
            "reason": reason,
            "intent": intent,
        }
    }


def _resolve_executor():
    """
    Try multiple known execution entrypoints without breaking compatibility.
    Returns a callable execute(intent: dict) -> dict
    """
    # 1) Preferred policy-aware executor
    try:
        from edge_policy_aware_executor import execute_intent as fn  # type: ignore
        return fn
    except Exception:
        pass

    # 2) Some builds route via broker_router
    try:
        from broker_router import execute_intent as fn  # type: ignore
        return fn
    except Exception:
        pass

    # 3) Some builds expose a generic route/dispatch
    try:
        from broker_router import route_intent as fn  # type: ignore
        return fn
    except Exception:
        pass

    return None


def _execute_one(cmd: Dict[str, Any]) -> None:
    cid = _cmd_id(cmd)
    if not cid:
        return

    if _seen_ok(cid):
        return

    if EDGE_HOLD:
        _ack_ok(cid, _simulate_receipt(cmd, reason="EDGE_HOLD"))
        return

    # intent-level override supported, but edge mode is the top-level safety rail
    intent = cmd.get("intent") or {}
    intent_mode = str(intent.get("mode") or "").lower().strip()
    effective_mode = intent_mode or EDGE_MODE

    if EDGE_MODE != "live" or effective_mode != "live":
        _ack_ok(cid, _simulate_receipt(cmd, reason=f"mode=edge:{EDGE_MODE} intent:{effective_mode or 'unset'}"))
        return

    exec_fn = _resolve_executor()
    if exec_fn is None:
        _ack_err(cid, "no_executor_available", {"hint": "Missing edge_policy_aware_executor/broker_router execution entrypoint"})
        return

    try:
        receipt = exec_fn(intent)

        # normalize
        if isinstance(receipt, dict) and "normalized" in receipt:
            norm = receipt
        else:
            norm = {"normalized": receipt or {"status": "FILLED"}}

        _ack_ok(cid, norm)

    except Exception as e:
        tb = "".join(traceback.format_exception(type(e), e, e.__traceback__))
        # keep ack payload bounded
        tb_short = tb[-1500:] if len(tb) > 1500 else tb
        _ack_err(cid, "execute_failed", {"trace": tb_short})


def poll_once() -> int:
    res = pull(AGENT_ID, MAX_PULL)
    cmds: List[Dict[str, Any]] = (res or {}).get("commands") or []
    if not cmds:
        return 0

    for c in cmds:
        _execute_one(c)

    return len(cmds)


def run_forever() -> None:
    global _last_info_ts, _last_empty_ts
    base = _pick_env("BUS_BASE_URL", "CLOUD_BASE_URL", "BASE_URL", "PUBLIC_BASE_URL", default="")

    print(f"[edge] bus poller online — agent={AGENT_ID} mode={EDGE_MODE} hold={EDGE_HOLD} base={base}")

    while True:
        try:
            n = poll_once()

            now = time.time()
            if n > 0:
                if now - _last_info_ts > 5:
                    print(f"[edge] pulled={n}")
                    _last_info_ts = now
            else:
                # periodic heartbeat so we can tell it's alive
                if now - _last_empty_ts > 60:
                    print("[edge] pulled=0 (idle)")
                    _last_empty_ts = now

        except KeyboardInterrupt:
            raise
        except Exception as e:
            # do not crash loop
            msg = str(e) or e.__class__.__name__
            print(f"[edge] ERROR poll loop: {msg}")

        time.sleep(PULL_PERIOD)


def start_bus_poller():
    # Back-compat helper if edge_agent imports this
    import threading
    t = threading.Thread(target=run_forever, name="bus-poller", daemon=True)
    t.start()
    return t


if __name__ == "__main__":
    run_forever()
