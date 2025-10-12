#!/usr/bin/env python3
# edge_agent.py — pulls commands from cloud, executes locally, ACKs with HMAC, posts receipts
import os, time, json, hmac, hashlib, requests, sys, traceback
from executors.coinbase_advanced_executor import execute_market_order as cb_exec
from executors.binance_us_executor import execute_market_order as bus_exec
from executors.kraken_executor import execute_market_order as kr_exec

# --- Env ---------------------------------------------------------------------
CLOUD_BASE_URL = os.getenv("CLOUD_BASE_URL") or os.getenv("BASE_URL") or "http://localhost:10000"
AGENT_ID       = (os.getenv("AGENT_ID") or "edge-local").split(",")[0].strip()
EDGE_MODE      = (os.getenv("EDGE_MODE") or "dryrun").strip().lower()   # dryrun | live
EDGE_HOLD      = (os.getenv("EDGE_HOLD") or "false").strip().lower() in {"1","true","yes"}
EDGE_SECRET    = (os.getenv("EDGE_SECRET") or "").strip()
EDGE_POLL_SECS = int(os.getenv("EDGE_POLL_SECS") or "10")

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "NovaTrade-Edge/1.0"})

EXECUTORS = {
    "COINBASE": cb_exec, "COINBASE_ADV": cb_exec, "CBADV": cb_exec,
    "BINANCEUS": bus_exec, "BINANCE_US": bus_exec, "BINANCE-US": bus_exec, "BUSA": bus_exec,
    "KRAKEN": kr_exec,
}
# --- Utils -------------------------------------------------------------------
def _log(msg: str):
    ts = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
    print(f"[edge] {ts} {msg}", flush=True)

def _canon(d: dict) -> bytes:
    # Canonical JSON for HMAC (stable ordering + minimal separators)
    return json.dumps(d, separators=(",", ":"), sort_keys=True).encode("utf-8")

def _hmac_hex(secret: str, raw_bytes: bytes) -> str:
    return hmac.new(secret.encode("utf-8"), raw_bytes, hashlib.sha256).hexdigest()

def _post_signed(url: str, body: dict, timeout=20):
    """
    POST with HMAC header. Safe to use even when server doesn't strictly require HMAC.
    """
    raw = _canon(body)
    ts  = str(int(time.time() * 1000))  # optional timestamp header
    sig = _hmac_hex(EDGE_SECRET, raw) if EDGE_SECRET else ""
    headers = {"Content-Type": "application/json"}
    if EDGE_SECRET:
        headers.update({"X-Signature": sig, "X-Timestamp": ts})
    return SESSION.post(url, data=raw, headers=headers, timeout=timeout)

def _post_signed_retry(url: str, body: dict, timeout=20, retries=3):
    """
    HMAC-signed POST with small retry for transient network/server hiccups (>=500).
    """
    for attempt in range(retries):
        try:
            r = _post_signed(url, body, timeout=timeout)
            # retry only on 5xx; 4xx means permanent issue
            if r.status_code < 500:
                return r
            _log(f"POST retry {attempt+1}/{retries} on {url}: {r.status_code}")
        except Exception as e:
            _log(f"POST exception {attempt+1}/{retries} on {url}: {e}")
        time.sleep(2 * (attempt + 1))
    # final attempt (let exceptions surface)
    return _post_signed(url, body, timeout=timeout)

# --- Minimal execution (stubbed) ---------------------------------------------
def exec_command(cmd: dict) -> dict:
    p = cmd.get("payload") or {}
    venue   = (p.get("venue") or "").upper()
    side    = (p.get("side") or "").upper()
    symbol  = p.get("symbol") or p.get("product_id") or ""
    amount_quote = float(p.get("amount") or p.get("quote_amount") or 0)
    amount_base  = float(p.get("base_amount") or 0)

    if EDGE_HOLD:
        return {"status":"held","message":"EDGE_HOLD enabled","fills":[]}

    if venue in EXECUTORS:
        return EXECUTORS[venue](venue_symbol=symbol, side=side,
                                amount_quote=amount_quote, amount_base=amount_base,
                                client_id=str(cmd.get("id")), edge_mode=EDGE_MODE, edge_hold=EDGE_HOLD)

    # fallback (dryrun)
    if EDGE_MODE != "live":
        px = 60000.0; qty = amount_base or (amount_quote/px if amount_quote else 0)
        return {"status":"ok","txid":f"SIM-{int(time.time()*1000)}","fills":[{"qty":qty,"price":px}],
                "venue":venue,"symbol":symbol,"side":side,"executed_qty":qty,"avg_price":px}
    return {"status":"error","message": f"live mode venue not implemented: {venue}", "fills":[]}

# --- ACK with header HMAC ----------------------------------------------------
def ack_command(cmd_id: int, result: dict):
    if not EDGE_SECRET:
        _log("WARN: EDGE_SECRET missing; cannot ACK with HMAC")
        return False

    body = {
        "id": cmd_id,
        "agent_id": AGENT_ID,
        "ok": (result.get("status") == "ok"),
        "status": result.get("status"),
        "txid": result.get("txid"),
        "fills": result.get("fills", []),
        "message": result.get("message"),
        "result": result or {},
        "ts": int(time.time() * 1000),
    }
    raw = _canon(body)
    sig = _hmac_hex(EDGE_SECRET, raw)
    headers = {"Content-Type": "application/json", "X-Signature": sig, "X-Timestamp": str(body["ts"])}
    url = CLOUD_BASE_URL.rstrip("/") + "/api/commands/ack"

    # retry on 5xx only
    for attempt in range(3):
        try:
            r = SESSION.post(url, data=raw, headers=headers, timeout=20)
            if r.status_code < 500:
                if r.status_code >= 400:
                    _log(f"ack failed {cmd_id}: {r.status_code} {r.text}")
                    return False
                _log(f"ack ok {cmd_id}: {r.status_code} {r.text}")
                return True
            _log(f"ack retry {attempt+1}/3 for {cmd_id}: {r.status_code}")
        except Exception as e:
            _log(f"ack exception {attempt+1}/3 for {cmd_id}: {e}")
        time.sleep(2 * (attempt + 1))
    # final hard fail
    try:
        r = SESSION.post(url, data=raw, headers=headers, timeout=20)
        if r.status_code < 400:
            _log(f"ack ok {cmd_id}: {r.status_code} {r.text}")
            return True
        _log(f"ack failed {cmd_id}: {r.status_code} {r.text}")
    except Exception as e:
        _log(f"ack final exception for {cmd_id}: {e}")
    return False

# --- Receipts: post to /api/receipts/ack (idempotent server-side) -----------
def post_receipt(cmd: dict, exec_result: dict):
    """
    Best-effort mirror of execution into cloud receipts → Google Sheet.
    Server is idempotent on 'id', so retries won't double-log.
    """
    if not EDGE_SECRET:
        _log("WARN: EDGE_SECRET missing; skipping receipt post")
        return

    payload = {
        "id": cmd["id"],
        "agent_id": AGENT_ID,
        "venue": (cmd.get("payload") or {}).get("venue") or exec_result.get("venue"),
        "symbol": (cmd.get("payload") or {}).get("symbol") or exec_result.get("symbol"),
        "side": (cmd.get("payload") or {}).get("side") or exec_result.get("side"),
        "status": "ok" if exec_result.get("status") == "ok" else exec_result.get("status", "error"),
        "txid": exec_result.get("txid",""),
        "fills": exec_result.get("fills", []),  # [{"price": "...", "qty": "..."}]
        "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "note": exec_result.get("message",""),
    }
    canon = _canon(payload)
    payload["hmac"] = _hmac_hex(EDGE_SECRET, canon)  # sign over canonical payload (without 'hmac')

    url = CLOUD_BASE_URL.rstrip("/") + "/api/receipts/ack"
    try:
        r = SESSION.post(url, json=payload, timeout=15)
        _log(f"posted receipt {cmd['id']}: {r.status_code} {r.text[:120]}")
    except Exception as e:
        _log(f"receipt post failed for {cmd['id']}: {e}")

# --- Poll loop ---------------------------------------------------------------
def pull_once(max_n=5):
    url  = CLOUD_BASE_URL.rstrip("/") + "/api/commands/pull"
    body = {"agent_id": AGENT_ID, "max": max_n}
    r = _post_signed(url, body, timeout=20)  # always signed (works with or without REQUIRE_HMAC_PULL)
    r.raise_for_status()
    items = r.json()
    return items if isinstance(items, list) else []

def main():
    _log(f"online — mode={EDGE_MODE} hold={EDGE_HOLD} base={CLOUD_BASE_URL} agent={AGENT_ID}")
    backoff = 2
    while True:
        try:
            cmds = pull_once()
            if cmds:
                _log(f"pulled {len(cmds)} command(s)")
            for cmd in cmds:
                cid = cmd.get("id")
                try:
                    res = exec_command(cmd)
                except Exception as e:
                    _log(f"exec error {cid}: {e}")
                    traceback.print_exc()
                    res = {"status": "error", "message": str(e), "fills": []}

                ack_ok = False
                try:
                    ack_ok = ack_command(cid, res)
                except Exception as e:
                    _log(f"ack exception {cid}: {e}")

                # Post receipt only after a successful ACK (keeps bus as source of truth)
                if ack_ok:
                    try:
                        post_receipt(cmd, res)
                    except Exception as e:
                        _log(f"post_receipt exception {cid}: {e}")

            backoff = 2
        except requests.HTTPError as he:
            try:
                code = he.response.status_code
                txt  = he.response.text[:200]
            except Exception:
                code, txt = -1, str(he)
            _log(f"poll HTTP {code}: {txt}")
        except Exception as e:
            _log(f"poll error: {e}")
        time.sleep(EDGE_POLL_SECS if backoff <= 2 else min(backoff, 30))
        backoff = min(backoff * 2, 30)

if __name__ == "__main__":
    main()
