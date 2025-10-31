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
SECRET   = os.getenv("OUTBOX_SECRET", "")
EDGE_MODE = os.getenv("EDGE_MODE", "dry").lower()
EDGE_HOLD = os.getenv("EDGE_HOLD", "false").lower() in ("1","true","yes")

PULL_PERIOD = int(os.getenv("PULL_PERIOD_SECONDS", "8"))
LEASE_SECONDS = int(os.getenv("LEASE_SECONDS", "90"))
MAX_PULL = int(os.getenv("MAX_CMDS_PER_PULL", "5"))

RECEIPTS_PATH = os.getenv("RECEIPTS_PATH", "./receipts.jsonl")

def _canonical(d: Dict[str, Any]) -> bytes:
    return json.dumps(d, separators=(",",":"), sort_keys=True).encode("utf-8")

def _sign(d: Dict[str, Any]) -> str:
    if not SECRET:
        return ""
    return hmac.new(SECRET.encode("utf-8"), _canonical(d), hashlib.sha256).hexdigest()

def _post(path: str, body: Dict[str, Any]) -> Dict[str, Any]:
    headers = {"Content-Type":"application/json"}
    if SECRET:
        headers["X-NT-Sig"] = _sign(body)
    r = requests.post(f"{BASE_URL}{path}", json=body, headers=headers, timeout=15)
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
def _ack(command_id: str, status: str, detail: Dict[str, Any]):
    body = {"agent_id": AGENT_ID, "command_id": command_id, "status": status, "detail": detail}
    try:
        _post("/api/commands/ack", body)
    except Exception as e:
        # Persist locally so we can reconcile later
        _append_receipt({"command_id": command_id, "status": status, "detail": detail, "ack_error": str(e)})
        return
    _append_receipt({"command_id": command_id, "status": status, "detail": detail})

def _execute(cmd: Dict[str, Any]) -> None:
    cid = cmd["id"]
    pay = cmd["payload"]
    venue  = pay.get("venue","").upper()
    symbol = pay.get("symbol","").upper()
    side   = pay.get("side","").lower()
    amount = float(pay.get("amount", 0) or 0.0)

    # Idempotency: if we already acked ok before, skip
    if _seen_ok(cid):
        _ack(cid, "skipped", {"reason":"duplicate (already ok)"})
        return

    if EDGE_HOLD:
        _ack(cid, "skipped", {"reason":"EDGE_HOLD"})
        return

    if amount <= 0 or side not in ("buy","sell") or not venue or not symbol:
        _ack(cid, "error", {"error":"invalid payload", "payload": pay})
        return

    try:
        if EDGE_MODE == "live":
            detail = _exec_live(venue, symbol, side, amount)
        else:
            detail = _exec_dry(venue, symbol, side, amount)
        _ack(cid, "ok", detail)
    except Exception as e:
        _ack(cid, "error", {"error": str(e), "payload": pay})

def poll_once():
    body = {"agent_id": AGENT_ID, "max": MAX_PULL, "lease_seconds": LEASE_SECONDS}
    try:
        res = _post("/api/commands/pull", body)
    except Exception as e:
        print(f"[edge] pull error: {e}")
        return

    cmds = (res or {}).get("commands", [])
    if not cmds:
        return
    print(f"[edge] received {len(cmds)} command(s)")
    for c in cmds:
        try:
            _execute(c)
        except Exception as e:
            # Defensive ack to avoid re-loops without detail
            try:
                _ack(c.get("id","?"), "error", {"error": f"unhandled: {e}"})
            except Exception:
                pass

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
