#!/usr/bin/env python3
# edge_agent.py — NovaTrade Edge Agent (Phase 5)
# - Polls commands from Bus, executes locally, ACKs with HMAC, posts receipts
# - Works with both legacy pull format (list) and new format {"ok":true,"commands":[...]}

import os, time, json, hmac, hashlib, requests, sys, traceback, re, collections

# --- Venue executors (existing modules) --------------------------------------
from executors.coinbase_advanced_executor import execute_market_order as cb_exec
from executors.binance_us_executor import execute_market_order as bus_exec
from executors.kraken_executor import execute_market_order as kr_exec
from executors.coinbase_advanced_executor import CoinbaseCDP
from executors.binance_us_executor import BinanceUS
from executors.kraken_executor import _balance as kraken_balance

# --- Optional telemetry (Phase 4) -------------------------------------------
try:
    import telemetry_db
except Exception:
    telemetry_db = None

try:
    import telemetry_sync
except Exception:
    telemetry_sync = None

# --- Env ---------------------------------------------------------------------
CLOUD_BASE_URL = os.getenv("CLOUD_BASE_URL") or os.getenv("BASE_URL") or "http://localhost:10000"
AGENT_ID       = (os.getenv("AGENT_ID") or os.getenv("EDGE_AGENT_ID") or "edge-local").split(",")[0].strip()
EDGE_MODE      = (os.getenv("EDGE_MODE") or "dryrun").strip().lower()   # dryrun | live
EDGE_HOLD      = (os.getenv("EDGE_HOLD") or "false").strip().lower() in {"1","true","yes"}
EDGE_SECRET    = (os.getenv("EDGE_SECRET") or "").strip()
EDGE_POLL_SECS = int(os.getenv("EDGE_POLL_SECS") or "10")

# Heartbeat / telemetry cadence
HEARTBEAT_SECS = int(os.getenv("HEARTBEAT_SECS") or os.getenv("HEARTBEAT_EVERY_S") or "900")  # default 15m
BALANCE_SNAPSHOT_SECS = int(os.getenv("BALANCE_SNAPSHOT_SECS", "7200"))  # every 2h
TELEMETRY_VERBOSE = (os.getenv("TELEMETRY_VERBOSE") or "0").strip() in {"1","true","yes"}

# --- HTTP session with retry -------------------------------------------------
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "NovaTrade-Edge/1.4"})
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
_retry = Retry(total=3, connect=3, read=3, backoff_factor=0.5,
               status_forcelist=(502, 503, 504),
               allowed_methods=frozenset(["GET", "POST"]))
SESSION.mount("https://", HTTPAdapter(max_retries=_retry))
SESSION.mount("http://",  HTTPAdapter(max_retries=_retry))

# --- Executors registry ------------------------------------------------------
EXECUTORS = {
    "COINBASE":    cb_exec,
    "COINBASEADV": cb_exec,
    "CBADV":       cb_exec,
    "BINANCEUS":   bus_exec,
    "BUSA":        bus_exec,
    "KRAKEN":      kr_exec,
}

# --- Utils -------------------------------------------------------------------
def _log(msg: str):
    ts = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
    print(f"[edge] {ts} {msg}", flush=True)

def _canon(d: dict) -> bytes:
    return json.dumps(d, separators=(",", ":"), sort_keys=True).encode("utf-8")

def _hmac_hex(secret: str, raw_bytes: bytes) -> str:
    return hmac.new(secret.encode("utf-8"), raw_bytes, hashlib.sha256).hexdigest()

def _post_signed(url: str, body: dict, timeout=20):
    raw = _canon(body)
    ts  = str(int(time.time() * 1000))
    headers = {"Content-Type": "application/json"}
    if EDGE_SECRET:
        headers["X-Signature"] = _hmac_hex(EDGE_SECRET, raw)
        headers["X-Timestamp"] = ts
    return SESSION.post(url, data=raw, headers=headers, timeout=timeout)

def _post_signed_retry(url: str, body: dict, timeout=20, retries=3):
    for attempt in range(retries):
        try:
            r = _post_signed(url, body, timeout=timeout)
            if r.status_code < 500:
                return r
            _log(f"POST retry {attempt+1}/{retries} on {url}: {r.status_code}")
        except Exception as e:
            _log(f"POST exception {attempt+1}/{retries} on {url}: {e}")
        time.sleep(2 * (attempt + 1))
    return _post_signed(url, body, timeout=timeout)

def _venue_key(v: str) -> str:
    return re.sub(r"[^A-Z]", "", (v or "").upper())

# --- Normalize command payload ----------------------------------------------
def _normalize_payload(p: dict):
    """
    Make sense of different payload variants:
      - amount_usd (quote)
      - amount / quote_amount (quote)
      - base_amount (base)
    Returns tuple: (venue_key, side, symbol, amount_quote, amount_base)
    """
    venue_key = _venue_key(p.get("venue"))
    side      = (p.get("side") or "").upper()
    symbol    = p.get("symbol") or p.get("product_id") or ""

    # quote amount (USD/USDT/USDC)
    amount_quote = None
    for k in ("amount_usd", "quote_amount", "amount"):
        if p.get(k) is not None:
            try:
                amount_quote = float(p.get(k))
                break
            except Exception:
                pass
    if amount_quote is None:
        amount_quote = 0.0

    # base amount (sell)
    amount_base = 0.0
    if p.get("base_amount") is not None:
        try:
            amount_base = float(p.get("base_amount"))
        except Exception:
            amount_base = 0.0

    return venue_key, side, symbol, float(amount_quote or 0.0), float(amount_base or 0.0)

# --- Execution router --------------------------------------------------------
def exec_command(cmd: dict) -> dict:
    p = cmd.get("payload") or {}
    venue_key, side, symbol, amount_quote, amount_base = _normalize_payload(p)

    if EDGE_HOLD:
        return {"status":"held","message":"EDGE_HOLD enabled","fills":[]}

    # Guardrails
    if side == "BUY" and amount_quote <= 0:
        return {"status":"error","message":"BUY requires amount_quote > 0","fills":[]}
    if side == "SELL" and amount_base <= 0:
        return {"status":"error","message":"SELL requires amount_base > 0","fills":[]}

    # Live executors
    if venue_key in EXECUTORS:
        return EXECUTORS[venue_key](
            venue_symbol=symbol, side=side,
            amount_quote=amount_quote, amount_base=amount_base,
            client_id=str(cmd.get("id")), edge_mode=EDGE_MODE, edge_hold=EDGE_HOLD
        )

    # Fallback in dryrun
    if EDGE_MODE != "live":
        px = 60000.0
        qty = amount_base or (amount_quote/px if amount_quote else 0)
        return {
            "status":"ok",
            "txid":f"SIM-{int(time.time()*1000)}",
            "fills":[{"qty":qty,"price":px}],
            "venue":venue_key,
            "symbol":symbol,
            "side":side,
            "executed_qty":qty,
            "avg_price":px
        }
    return {"status":"error","message": f"live mode venue not implemented: {venue_key}", "fills":[]}

# --- ACK (new Bus format) ----------------------------------------------------
def ack_command_new(cmd_id: int, exec_result: dict):
    """
    New Bus ack: POST /api/commands/ack with body:
      { "id": <int>, "status": "DONE"|"ERROR"|"HELD", "receipt": {...} }
    Signed with X-Signature/X-Timestamp if EDGE_SECRET is set.
    """
    status = "DONE" if exec_result.get("status") in {"ok","held"} else "ERROR"
    body = {
        "id": cmd_id,
        "status": status,
        "receipt": {
            "agent_id": AGENT_ID,
            "status": exec_result.get("status"),
            "txid": exec_result.get("txid"),
            "fills": exec_result.get("fills", []),
            "message": exec_result.get("message"),
            "venue": exec_result.get("venue"),
            "symbol": exec_result.get("symbol"),
            "side": exec_result.get("side"),
            "result": exec_result,
            "ts": int(time.time())
        }
    }
    url = CLOUD_BASE_URL.rstrip("/") + "/api/commands/ack"
    r = _post_signed_retry(url, body, timeout=20, retries=3)
    if r.status_code >= 400:
        _log(f"ack NEW failed {cmd_id}: {r.status_code} {r.text[:160]}")
        return False
    _log(f"ack NEW ok {cmd_id}: {r.status_code}")
    return True

# --- Receipts: post to /api/receipts/ack ------------------------------------
def post_receipt(cmd: dict, exec_result: dict):
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
        "txid": exec_result.get("txid", ""),
        "fills": exec_result.get("fills", []),
        "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "note": exec_result.get("message", ""),
        # Optional provenance / balances
        "requested_symbol": exec_result.get("requested_symbol"),
        "resolved_symbol":  exec_result.get("resolved_symbol") or exec_result.get("symbol"),
        "post_balances":    exec_result.get("post_balances"),
    }
    canon = _canon({k: v for k, v in payload.items() if k != "hmac"})
    payload["hmac"] = _hmac_hex(EDGE_SECRET, canon)

    url = CLOUD_BASE_URL.rstrip("/") + "/api/receipts/ack"
    try:
        r = _post_signed_retry(url, payload, timeout=30, retries=3)
        _log(f"posted receipt {cmd['id']}: {r.status_code} {r.text[:160]}")
    except Exception as e:
        _log(f"receipt post failed for {cmd['id']}: {e}")

    if telemetry_db:
        try:
            telemetry_db.log_receipt(cmd_id=str(cmd.get("id")), receipt=exec_result)
            pb = exec_result.get("post_balances")
            if isinstance(pb, dict):
                telemetry_db.upsert_balances(venue=exec_result.get("venue"), bal_map=pb)
        except Exception as e:
            _log(f"telemetry mirror error: {e}")

# --- Telemetry / heartbeat ---------------------------------------------------
_last_hb = 0
_last_bal_snap = 0

def maybe_telemetry_tick():
    global _last_hb
    if HEARTBEAT_SECS <= 0:
        return
    now = time.time()
    if now - _last_hb < HEARTBEAT_SECS:
        return
    _last_hb = now
    if telemetry_db:
        try:
            telemetry_db.log_heartbeat(agent=AGENT_ID, ok=True, latency_ms=0)
            if TELEMETRY_VERBOSE:
                _log("telemetry_db: heartbeat row inserted")
        except Exception as e:
            _log(f"telemetry_db heartbeat error: {e}")
    if telemetry_sync:
        try:
            hb = telemetry_sync.send_heartbeat(latency_ms=0)
            if TELEMETRY_VERBOSE:
                _log(f"heartbeat push: {hb}")
        except Exception as e:
            _log(f"heartbeat push error: {e}")
        try:
            tp = telemetry_sync.push_telemetry()
            if TELEMETRY_VERBOSE:
                _log(f"telemetry push: {tp}")
        except Exception as e:
            _log(f"telemetry push error: {e}")

def maybe_balance_snapshot():
    global _last_bal_snap
    if not telemetry_db:
        return
    now = time.time()
    if now - _last_bal_snap < BALANCE_SNAPSHOT_SECS:
        return
    _last_bal_snap = now
    # Coinbase
    try:
        cb = CoinbaseCDP()
        bals = cb.balances()
        telemetry_db.upsert_balances("COINBASE", bals)
        _log(f"snapshot COINBASE balances: { {k: round(v,8) for k,v in bals.items() if k in ('USDC','BTC')} }")
    except Exception as e:
        _log(f"snapshot COINBASE error: {e}")
    # Binance.US
    try:
        bus = BinanceUS()
        acct = bus.account()
        bals = { (b.get('asset') or '').upper(): float(b.get('free') or 0.0) for b in (acct.get('balances') or []) }
        telemetry_db.upsert_balances("BINANCEUS", bals)
        _log(f"snapshot BINANCEUS balances: { {k: round(v,8) for k,v in bals.items() if k in ('USDT','BTC')} }")
    except Exception as e:
        _log(f"snapshot BINANCEUS error: {e}")
    # Kraken
    try:
        bals = kraken_balance()
        telemetry_db.upsert_balances("KRAKEN", bals)
        _log(f"snapshot KRAKEN balances: { {k: round(v,8) for k,v in bals.items() if k in ('USDT','XBT')} }")
    except Exception as e:
        _log(f"snapshot KRAKEN error: {e}")

# --- Pull loop (supports legacy & new) ---------------------------------------
def _parse_pull_response(r):
    """
    Accepts either:
      - legacy: [ {id, payload, ...}, ... ]
      - new: {"ok":true,"commands":[{id,ts,payload},...]}
    Returns list of command dicts.
    """
    try:
        j = r.json()
    except Exception:
        return []
    if isinstance(j, list):
        return j
    if isinstance(j, dict) and isinstance(j.get("commands"), list):
        return j["commands"]
    return []

def pull_once(limit=10):
    url  = CLOUD_BASE_URL.rstrip("/") + "/api/commands/pull"
    body = {"agent_id": AGENT_ID, "limit": limit}
    r = _post_signed(url, body, timeout=20)
    if r.status_code >= 400:
        raise requests.HTTPError(f"{r.status_code} {r.text[:200]}", response=r)
    return _parse_pull_response(r)

# --- Idempotency cache for recent command IDs --------------------------------
RECENT_IDS = collections.deque(maxlen=256)
def _seen_before(cid):
    s = str(cid)
    if s in RECENT_IDS:
        return True
    RECENT_IDS.append(s)
    return False

# --- Main --------------------------------------------------------------------
def main():
    _log(f"online — mode={EDGE_MODE} hold={EDGE_HOLD} base={CLOUD_BASE_URL} agent={AGENT_ID}")
    backoff = 2
    while True:
        try:
            if EDGE_HOLD:
                time.sleep(EDGE_POLL_SECS)
                continue

            cmds = pull_once(limit=10)
            if cmds:
                _log(f"pulled {len(cmds)} command(s)")

            for cmd in cmds:
                cid = cmd.get("id")
                if _seen_before(cid):
                    _log(f"skip duplicate id {cid}")
                    continue

                try:
                    res = exec_command(cmd)
                except Exception as e:
                    _log(f"exec error {cid}: {e}")
                    traceback.print_exc()
                    res = {"status": "error", "message": str(e), "fills": []}

                try:
                    ok = ack_command_new(cid, res)
                except Exception as e:
                    _log(f"ack exception {cid}: {e}")
                    ok = False

                if ok:
                    try:
                        post_receipt(cmd, res)
                    except Exception as e:
                        _log(f"post_receipt exception {cid}: {e}")

            maybe_telemetry_tick()
            maybe_balance_snapshot()
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
