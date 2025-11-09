#!/usr/bin/env python3
"""
edge_bus_poller.py — NovaTrade Edge → Bus poll/exec/ack loop

• Pulls signed commands from Bus (/api/commands/pull)
• Executes orders (live or dry-run) with simple venue router
• Acks results (/api/commands/ack) with HMAC
• Idempotent (receipts.jsonl) so duplicate pulls don’t double-execute
• Honors EDGE_HOLD (skip) and EDGE_MODE=dry|live

Environment (Edge worker):
  BASE_URL=https://novatrade3-0.onrender.com
  AGENT_ID=edge-primary
  OUTBOX_SECRET=...            # must match Bus
  EDGE_MODE=live|dry           # default dry
  EDGE_HOLD=false|true         # default false
  PULL_PERIOD_SECONDS=8
  LEASE_SECONDS=90
  MAX_CMDS_PER_PULL=5
  # Venue API keys loaded by your existing local executors (optional)
"""

import os, time, json, hmac, hashlib, requests, threading
from typing import Dict, Any

BASE_URL = os.getenv("BASE_URL", "http://localhost:10000")
AGENT_ID = os.getenv("AGENT_ID", "edge-primary")
SECRET   = os.getenv("EDGE_SECRET", "")
EDGE_MODE = os.getenv("EDGE_MODE", "dry").lower()
EDGE_HOLD = os.getenv("EDGE_HOLD", "false").lower() in ("1","true","yes")

PULL_PERIOD = int(os.getenv("PULL_PERIOD_SECONDS", "8"))
LEASE_SECONDS = int(os.getenv("LEASE_SECONDS", "90"))
MAX_PULL = int(os.getenv("EDGE_PULL_LIMIT", "3"))

RECEIPTS_PATH = os.getenv("RECEIPTS_PATH", "./receipts.jsonl")

def _raw_json(d: Dict[str, Any]) -> str:
    return json.dumps(d, separators=(",", ":"))

def _sign_raw(d: Dict[str, Any]) -> str:
    if not SECRET:
        return ""
    raw = _raw_json(d)
    return hmac.new(SECRET.encode("utf-8"), raw.encode("utf-8"), hashlib.sha256).hexdigest()

def _post(path: str, body: Dict[str, Any]) -> Dict[str, Any]:
    raw = _raw_json(body)                  # exact string we will send
    sig = _sign_raw(body)
    headers = {
        "Content-Type": "application/json",
        "X-Nova-Signature": sig,          # header Bus verifies
    }
    r = requests.post(f"{BASE_URL}{path}", data=raw, headers=headers, timeout=15)
    r.raise_for_status()
    return r.json()
  
def _append_receipt(line: Dict[str, Any]) -> None:
    try:
        with open(RECEIPTS_PATH, "a", encoding="utf-8") as f:
            f.write(json.dumps(line, ensure_ascii=False) + "\n")
    except Exception:
        pass

def _seen_ok(command_id: str) -> bool:
    try:
        with open(RECEIPTS_PATH, "r", encoding="utf-8") as f:
            for ln in f:
                try:
                    j = json.loads(ln)
                    if j.get("command_id")==command_id and j.get("status")=="ok":
                        return True
                except Exception:
                    continue
    except FileNotFoundError:
        return False
    return False

# ---------- venue executors (plug in your real ones here) ----------
def _exec_live(venue: str, symbol: str, side: str, amount: float) -> Dict[str, Any]:
    """
    Replace these try/except blocks with your real adapters.
    Must be FAST (<= ~2-3s) to avoid lease expiry; increase LEASE_SECONDS if needed.
    Return dict detail suitable for ACK (ids, fills, cost, etc.)
    """
    venue = venue.upper()
    try:
        if venue == "KRAKEN":
            # from kraken_executor import place_order
            # oid = place_order(symbol, side, amount)
            oid = f"kraken-{int(time.time())}"
            return {"venue":"KRAKEN","order_id":oid,"symbol":symbol,"side":side,"amount":amount}
        elif venue == "COINBASE":
            # from coinbase_executor import place_order
            oid = f"coinbase-{int(time.time())}"
            return {"venue":"COINBASE","order_id":oid,"symbol":symbol,"side":side,"amount":amount}
        elif venue == "BINANCEUS":
            # from binanceus_executor import place_order
            oid = f"binanceus-{int(time.time())}"
            return {"venue":"BINANCEUS","order_id":oid,"symbol":symbol,"side":side,"amount":amount}
        else:
            raise RuntimeError(f"unknown venue {venue}")
    except Exception as e:
        raise RuntimeError(f"execution failed: {e}")

def _exec_dry(venue: str, symbol: str, side: str, amount: float) -> Dict[str, Any]:
    return {
        "venue": venue.upper(),
        "dry_run": True,
        "symbol": symbol.upper(),
        "side": side.lower(),
        "amount": float(amount),
        "ts": int(time.time()),
    }

# ---------- core loop ----------
from edge_bus_client import pull, ack

res = pull(AGENT_ID, 1)
cmds = (res or {}).get("commands") or []
if not cmds:
    # nothing leased (or transient error) – just sleep and loop
    time.sleep(POLL_SECS)
    continue

def ack_success(agent_id: str, cmd_id: int | str, venue: str, symbol: str, side: str,
                executed_qty: float, avg_price: float, extras: dict | None = None):
    receipt = {
        "normalized": {
            "receipt_id": f"{agent_id}:{cmd_id}",
            "venue": venue,
            "symbol": symbol,
            "side": side,
            "executed_qty": executed_qty,
            "avg_price": avg_price,
            "status": "FILLED",
        }
    }
    if extras:  # fee, order_id, txid, quote_spent, etc.
        receipt["normalized"].update(extras)
    return ack(agent_id, cmd_id, True, receipt)

def ack_error(agent_id: str, cmd_id: int | str, message: str, extras: dict | None = None):
    receipt = {"error": message}
    if extras:
        receipt.update(extras)
    return ack(agent_id, cmd_id, False, receipt)

  
def poll_once():
    if EDGE_HOLD:
        print("[edge] hold=True; skipping poll")
        return
    try:
        res = pull(AGENT_ID, MAX_PULL)
    except Exception as e:
        print(f"[edge] pull error: {e}")
        return

    cmds: List[Dict[str, Any]] = (res or {}).get("commands") or []
    if not cmds:
        return
    print(f"[edge] received {len(cmds)} command(s)")
    for c in cmds:
        try:
            _execute(c)
        except Exception as e:
            cid = str(c.get("id", "?"))
            tb = "".join(traceback.format_exception_only(type(e), e)).strip()
            try:
                ack(AGENT_ID, cid, False, {"error": f"unhandled: {tb}"})
            except Exception:
                pass

def _execute(cmd: Dict[str, Any]) -> None:
    cid = cmd["id"]
    intent = cmd.get("intent", {})
    venue  = (intent.get("venue") or "").upper()
    symbol = (intent.get("symbol") or "").upper()
    side   = (intent.get("side") or "").lower()
    amount = float(intent.get("amount") or 0.0)

    # … call your venue executor(s) here …
    # detail should be a dict with normalized receipt:
    detail = {"normalized":{
        "receipt_id": f"{AGENT_ID}:{cid}",
        "venue": venue,
        "symbol": symbol,
        "side": side,
        "executed_qty": amount,
        "avg_price": 0.0,   # fill from executor
        "status": "FILLED" if EDGE_MODE=="live" else "SIMULATED",
    }}

    ok = True
    ack_success(AGENT_ID, cid, venue, symbol, side, executed_qty=amount, avg_price=price, extras={"order_id": oid})

    # On error:
    ack_error(AGENT_ID, cid, f"price fetch failed: {err}", {"venue": venue, "symbol": symbol})

def run_forever():
    print(f"[edge] bus poller online — agent={AGENT_ID} mode={EDGE_MODE} hold={EDGE_HOLD} base={BASE_URL}")
    while True:
        poll_once()
        time.sleep(PULL_PERIOD)

# For import-as-thread usage in your existing edge_agent.py
def start_bus_poller():
    t = threading.Thread(target=run_forever, name="bus-poller", daemon=True)
    t.start()
    return t

if __name__ == "__main__":
    run_forever()
