#!/usr/bin/env python3
# edge_agent.py — NovaTrade Edge Agent (patched to Bus Command Bus spec)
#
# Roles:
# - Poll commands from Bus (/api/commands/pull) with HMAC (X-NT-Sig)
# - Execute intents on Coinbase Advanced, BinanceUS, Kraken
# - Support BUY, SELL, and SWAP (with simple USDT/USDC bridge logic)
# - ACK results to /api/commands/ack → {"agent_id","command_id","status","detail"}
# - Heartbeat + periodic balance snapshots
# - Telemetry: push balances to Bus (/api/telemetry/push) with optional HMAC
#
# Env (Edge):
#   BASE_URL or CLOUD_BASE_URL=https://novatrade3-0.onrender.com
#   AGENT_ID=edge-primary
#   EDGE_MODE=live|dryrun
#   EDGE_HOLD=false
#   OUTBOX_SECRET=<shared HMAC with Bus>  # preferred
#   EDGE_SECRET=<legacy name, used if OUTBOX_SECRET unset>
#   TELEMETRY_SECRET=<optional; defaults to OUTBOX_SECRET/EDGE_SECRET>
#   EDGE_POLL_SECS=8
#   LEASE_SECONDS=120
#   MAX_CMDS_PER_PULL=5
#   HEARTBEAT_SECS=900
#   BALANCE_SNAPSHOT_SECS=7200
#   PUSH_BALANCES_ENABLED=1
#   PUSH_BALANCES_EVERY_S=600
#   TELEMETRY_VERBOSE=0
#   WALLET_MONITOR_ENDPOINT=/api/telemetry/push   # default; aliases also work on Bus

import os, time, json, hmac, hashlib, requests, re, traceback, collections
from typing import Dict, Any

# --- Venue executors ---------------------------------------------------------
from executors.coinbase_advanced_executor import execute_market_order as cb_exec
from executors.binance_us_executor import execute_market_order as bus_exec
from executors.kraken_executor import execute_market_order as kr_exec
from executors.coinbase_advanced_executor import CoinbaseCDP
from executors.binance_us_executor import BinanceUS
from executors.kraken_executor import _balance as kraken_balance
from edge_pretrade import pretrade_validate

# --- Optional telemetry stores ----------------------------------------------
try:
    import telemetry_db
except Exception:
    telemetry_db = None
try:
    import telemetry_sync
except Exception:
    telemetry_sync = None

# --- Env ---------------------------------------------------------------------
BASE_URL = (os.getenv("CLOUD_BASE_URL") or os.getenv("BASE_URL") or "http://localhost:10000").rstrip("/")
AGENT_ID = (os.getenv("AGENT_ID") or os.getenv("EDGE_AGENT_ID") or "edge-primary").split(",")[0].strip()
EDGE_MODE = (os.getenv("EDGE_MODE") or "dryrun").strip().lower()      # dryrun | live
EDGE_HOLD = (os.getenv("EDGE_HOLD") or "false").strip().lower() in {"1","true","yes"}

OUTBOX_SECRET = (os.getenv("OUTBOX_SECRET") or os.getenv("EDGE_SECRET") or "").strip()
TELEMETRY_SECRET = (os.getenv("TELEMETRY_SECRET") or OUTBOX_SECRET).strip()

EDGE_POLL_SECS = int(os.getenv("EDGE_POLL_SECS") or "8")
LEASE_SECONDS = int(os.getenv("LEASE_SECONDS") or "120")
MAX_PULL = int(os.getenv("MAX_CMDS_PER_PULL") or "5")

HEARTBEAT_SECS = int(os.getenv("HEARTBEAT_SECS") or os.getenv("HEARTBEAT_EVERY_S") or "900")
BALANCE_SNAPSHOT_SECS = int(os.getenv("BALANCE_SNAPSHOT_SECS") or "7200")

PUSH_BALANCES_ENABLED = (os.getenv("PUSH_BALANCES_ENABLED") or "1").strip().lower() in {"1","true","yes"}
PUSH_BALANCES_EVERY_S = int(os.getenv("PUSH_BALANCES_EVERY_S") or "600")
TELEMETRY_VERBOSE = (os.getenv("TELEMETRY_VERBOSE") or "0").strip().lower() in {"1","true","yes"}
WALLET_MONITOR_ENDPOINT = (os.getenv("WALLET_MONITOR_ENDPOINT") or "/api/telemetry/push").strip()

# --- HTTP session w/ retry ---------------------------------------------------
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "NovaTrade-Edge/2.0"})
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
_retry = Retry(total=3, connect=3, read=3, backoff_factor=0.5,
               status_forcelist=(429, 502, 503, 504),
               allowed_methods=frozenset(["GET", "POST"]))
SESSION.mount("https://", HTTPAdapter(max_retries=_retry))
SESSION.mount("http://",  HTTPAdapter(max_retries=_retry))

# --- Logging -----------------------------------------------------------------
def _log(msg: str):
    ts = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
    print(f"[edge] {ts} {msg}", flush=True)

# --- HMAC helpers ------------------------------------------------------------
def _canonical(body: Dict[str, Any]) -> bytes:
    return json.dumps(body, separators=(",", ":"), sort_keys=True).encode("utf-8")

def _sign(secret: str, body: Dict[str, Any]) -> str:
    if not secret:
        return ""
    return hmac.new(secret.encode("utf-8"), _canonical(body), hashlib.sha256).hexdigest()

def _post_json(path: str, body: Dict[str, Any], secret: str = OUTBOX_SECRET, timeout=20) -> requests.Response:
    headers = {"Content-Type": "application/json", "User-Agent": "NovaTrade-Edge/2.0"}
    if secret:
        headers["X-NT-Sig"] = _sign(secret, body)
    url = f"{BASE_URL}{path}"
    return SESSION.post(url, json=body, headers=headers, timeout=timeout)

def _post_json_ok(path: str, body: Dict[str, Any], secret: str = OUTBOX_SECRET, timeout=20) -> Dict[str, Any]:
    r = _post_json(path, body, secret=secret, timeout=timeout)
    if r.status_code >= 400:
        raise requests.HTTPError(f"{r.status_code} {r.text[:200]}", response=r)
    try:
        return r.json()
    except Exception:
        return {}

# --- Executors registry ------------------------------------------------------
EXECUTORS = {
    "COINBASE":    cb_exec,
    "COINBASEADV": cb_exec,
    "CBADV":       cb_exec,
    "BINANCEUS":   bus_exec,
    "BUSA":        bus_exec,
    "KRAKEN":      kr_exec,
}

# --- Pair helpers ------------------------------------------------------------
def _venue_key(v: str) -> str:
    return re.sub(r"[^A-Z]", "", (v or "").upper())

def _kr_asset(a: str) -> str:
    a = (a or "").upper()
    return "XBT" if a == "BTC" else a

def _cb_prod(base: str, quote: str) -> str:
    return f"{base.upper()}-{quote.upper()}"

def _bus_sym(base: str, quote: str) -> str:
    return f"{base.upper()}{quote.upper()}"

def _kr_sym(base: str, quote: str) -> str:
    return f"{_kr_asset(base)}{_kr_asset(quote)}"

def resolve_symbol(venue_key: str, base: str, quote: str) -> str:
    v = _venue_key(venue_key)
    if v in ("COINBASE","COINBASEADV","CBADV"):
        return _cb_prod(base, quote)
    if v in ("BINANCEUS","BUSA"):
        return _bus_sym(base, quote)
    if v == "KRAKEN":
        return _kr_sym(base, quote)
    return f"{base.upper()}-{quote.upper()}"

# --- Public price fetchers ---------------------------------------------------
def fetch_price(venue_key: str, base: str, quote: str) -> float:
    v = _venue_key(venue_key)
    try:
        if v in ("BINANCEUS","BUSA"):
            sym = _bus_sym(base, quote)
            j = SESSION.get("https://api.binance.us/api/v3/ticker/price", params={"symbol": sym}, timeout=6).json()
            return float(j["price"])
        if v in ("COINBASE","COINBASEADV","CBADV"):
            prod = _cb_prod(base, quote)
            j = SESSION.get(f"https://api.exchange.coinbase.com/products/{prod}/ticker", timeout=6).json()
            return float(j["price"])
        if v == "KRAKEN":
            pair = _kr_sym(base, quote)
            j = SESSION.get("https://api.kraken.com/0/public/Ticker", params={"pair": pair}, timeout=6).json()
            if j.get("error"):
                raise RuntimeError(",".join(j["error"]))
            res = next(iter(j["result"].values()))
            return float(res["c"][0])
    except Exception as e:
        _log(f"price fetch failed {venue_key} {base}/{quote}: {e}")
    return float("nan")

# --- Balances (for telemetry & snapshots) -----------------------------------
def get_balances() -> dict:
    out = {}
    try:
        cb = CoinbaseCDP()
        out["COINBASE"] = cb.balances()
    except Exception as e:
        _log(f"balances COINBASE error: {e}")
    try:
        bus = BinanceUS()
        acct = bus.account()
        out["BINANCEUS"] = { (b.get('asset') or '').upper(): float(b.get('free') or 0.0)
                             for b in (acct.get('balances') or []) }
    except Exception as e:
        _log(f"balances BINANCEUS error: {e}")
    try:
        out["KRAKEN"] = kraken_balance()
    except Exception as e:
        _log(f"balances KRAKEN error: {e}")
    return out

def maybe_push_balances():
    if not PUSH_BALANCES_ENABLED:
        return
    now = time.time()
    if not hasattr(maybe_push_balances, "_last"):
        maybe_push_balances._last = 0.0
    if now - maybe_push_balances._last < max(60, PUSH_BALANCES_EVERY_S):
        return
    maybe_push_balances._last = now
    try:
        payload = {"agent_id": AGENT_ID, "balances": get_balances()}
        secret = TELEMETRY_SECRET or OUTBOX_SECRET
        r = _post_json(WALLET_MONITOR_ENDPOINT, payload, secret=secret, timeout=15)
        _log(f"push_balances {r.status_code} {r.text[:160]}")
    except Exception as e:
        _log(f"push_balances error: {e}")

# --- Payload normalization ---------------------------------------------------
def _normalize_payload(p: dict):
    venue_key = _venue_key(p.get("venue"))
    action    = (p.get("action") or p.get("side") or "").upper()
    side      = (p.get("side") or "").upper()
    symbol    = p.get("symbol") or p.get("product_id") or ""

    amount_quote = None
    for k in ("amount_quote","amount_usd","quote_amount","amount"):
        if p.get(k) is not None:
            try:
                amount_quote = float(p.get(k)); break
            except Exception: pass
    amount_quote = float(amount_quote or 0.0)

    amount_base = 0.0
    if p.get("base_amount") is not None:
        try: amount_base = float(p.get("base_amount"))
        except Exception: amount_base = 0.0

    return {
        "venue": venue_key,
        "action": action,
        "side": side,
        "symbol": symbol,
        "amount_quote": amount_quote,
        "amount_base": amount_base,
        "from": (p.get("from") or p.get("asset_from") or "").upper(),
        "to": (p.get("to") or p.get("asset_to") or p.get("quote") or "").upper(),
        "target_quote": (p.get("quote") or "").upper(),
        "client_note": p.get("note") or "",
    }

# --- SWAP helpers ------------------------------------------------------------
def _direct_or_bridge_paths(venue: str, from_asset: str, to_quote: str):
    from_a = (from_asset or "").upper(); to_q = (to_quote or "").upper()
    if not from_a or not to_q:
        return []
    if from_a == to_q:
        return [(from_a, to_q)]
    c = []
    c.append([(from_a, to_q)])
    if to_q != "USDT": c.append([(from_a, "USDT"), ("USDT", to_q)])
    if to_q != "USDC": c.append([(from_a, "USDC"), ("USDC", to_q)])
    if to_q in ("USDT","USDC"):
        other = "USDC" if to_q == "USDT" else "USDT"
        c.append([(from_a, other), (other, to_q)])
    return c

def _estimate_base_qty(venue: str, base: str, quote: str, want_quote_amount: float) -> float:
    px = fetch_price(venue, base, quote)
    if not (px and px == px and px > 0):
        raise RuntimeError(f"no price for {venue} {base}/{quote}")
    return (want_quote_amount / px) * 1.0025  # small safety factor

# --- Execution ---------------------------------------------------------------
def execute_market(venue_key: str, base: str, quote: str, side: str,
                   amount_quote: float = 0.0, amount_base: float = 0.0, client_id: str = ""):
    symbol = resolve_symbol(venue_key, base, quote)
    exe = EXECUTORS.get(_venue_key(venue_key))
    if not exe:
        raise RuntimeError(f"executor missing for {venue_key}")
    return exe(venue_symbol=symbol, side=side, amount_quote=amount_quote, amount_base=amount_base,
               client_id=str(client_id), edge_mode=EDGE_MODE, edge_hold=EDGE_HOLD)

def handle_swap(cmd: dict, pnorm: dict) -> dict:
    venue = pnorm["venue"]
    from_asset = pnorm["from"] or (pnorm["symbol"].split("-")[0] if pnorm["symbol"] else "")
    to_quote   = pnorm["to"] or pnorm["target_quote"] or "USDT"
    want_q     = max(0.0, float(pnorm["amount_quote"]))
    if not (venue and from_asset and to_quote and want_q > 0):
        return {"status":"error","message":"SWAP requires venue, from, to, amount_quote>0","fills":[]}

    if from_asset == to_quote:
        return {"status":"ok","message":"SWAP no-op: from==to","fills":[],
                "venue":venue,"symbol":resolve_symbol(venue, from_asset, to_quote),"side":"SELL",
                "executed_qty":0.0,"avg_price":0.0}

    paths = _direct_or_bridge_paths(venue, from_asset, to_quote)
    all_fills = []
    for path in paths:
        try:
            if len(path) == 1:
                base, quote = path[0]
                qty_base = _estimate_base_qty(venue, base, quote, want_q)
                res = execute_market(venue, base, quote, side="SELL", amount_base=qty_base, client_id=cmd.get("id"))
                if (res.get("status") or "") != "ok":
                    raise RuntimeError(res.get("message","unknown error"))
                out = res.copy()
                out.update({"status":"ok","message":"SWAP direct path executed","fills":res.get("fills",[])})
                return out
            elif len(path) == 2:
                (base1, quote1), (base2, quote2) = path
                if quote1 != base2: continue
                qty_bridge = _estimate_base_qty(venue, base2, quote2, want_q)
                qty_from   = _estimate_base_qty(venue, base1, quote1, qty_bridge)
                res1 = execute_market(venue, base1, quote1, side="SELL", amount_base=qty_from, client_id=cmd.get("id"))
                if (res1.get("status") or "") != "ok":
                    raise RuntimeError(res1.get("message","bridge leg 1 failed"))
                all_fills.extend(res1.get("fills",[]))
                received_bridge = float(res1.get("executed_qty", 0.0)) * float(res1.get("avg_price", 0.0))
                if received_bridge <= 0:
                    raise RuntimeError("zero bridge qty")
                res2 = execute_market(venue, base2, quote2, side="SELL", amount_base=received_bridge, client_id=cmd.get("id"))
                if (res2.get("status") or "") != "ok":
                    raise RuntimeError(res2.get("message","bridge leg 2 failed"))
                all_fills.extend(res2.get("fills",[]))
                symbol_final = resolve_symbol(venue, from_asset, to_quote)
                return {"status":"ok","message":f"SWAP bridge via {quote1} ok","fills":all_fills,
                        "venue":venue,"symbol":symbol_final,"side":"SELL"}
        except Exception as e:
            _log(f"SWAP path failed {venue} {path}: {e}")
            all_fills = []
            continue
    return {"status":"error","message":"SWAP failed: no viable path","fills":[]}

def exec_command(cmd: dict) -> dict:
    p = cmd.get("payload") or {}
    pnorm = _normalize_payload(p)
    venue = pnorm["venue"]
    action = pnorm["action"]
    side = pnorm["side"]
    symbol = pnorm["symbol"]

    if EDGE_HOLD:
        return {"status":"held","message":"EDGE_HOLD enabled","fills":[], "venue":venue, "symbol":symbol, "side":side or action}

    if action == "SWAP":
        return handle_swap(cmd, pnorm)

    if not side:
        side = "BUY" if pnorm["amount_quote"] > 0 else "SELL"
    if side == "BUY" and pnorm["amount_quote"] <= 0:
        return {"status":"error","message":"BUY requires amount_quote > 0","fills":[],"venue":venue,"symbol":symbol,"side":side}
    if side == "SELL" and pnorm["amount_base"] <= 0:
        return {"status":"error","message":"SELL requires base_amount > 0","fills":[],"venue":venue,"symbol":symbol,"side":side}
    # --- Pre-trade check & smart quote choice -----------------------------------
    base_sym, quote_sym = (symbol.split("-", 1) + [""])[:2] if "-" in symbol else (symbol, "USD")
    px = fetch_price(venue, base_sym, quote_sym)           # your existing best price helper
    venue_balances = get_balances().get(venue, {})         # your in-memory balances

    ok, reason, chosen_quote, min_qty, min_notional = pretrade_validate(
        venue=venue,
        base=base_sym,
        quote=quote_sym,
        price=px,
        amount_base=pnorm["amount_base"],
        amount_quote=pnorm["amount_quote"],
        venue_balances=venue_balances,
    )
    if not ok:
        return {
            "status":"error","message":reason,"fills":[],
            "venue":venue,"symbol":f"{base_sym}-{chosen_quote}","side":side,
            "executed_qty":None,"avg_price":None
        }

    # If we auto-changed quote, update symbol passed to executor
    if chosen_quote and chosen_quote != quote_sym:
        symbol = f"{base_sym}-{chosen_quote}"

    try:
        res = EXECUTORS[_venue_key(venue)](
            venue_symbol=resolve_symbol(venue, symbol.split("-")[0] if "-" in symbol else symbol, symbol.split("-")[1] if "-" in symbol else "USD"),
            side=side,
            amount_quote=pnorm["amount_quote"],
            amount_base=pnorm["amount_base"],
            client_id=str(cmd.get("id")),
            edge_mode=EDGE_MODE,
            edge_hold=EDGE_HOLD
        )
    except Exception as e:
        return {"status":"error","message":str(e),"fills":[],"venue":venue,"symbol":symbol,"side":side}

    res.setdefault("venue", venue)
    res.setdefault("symbol", symbol)
    res.setdefault("side", side)
    return res

# --- ACK & receipts ----------------------------------------------------------
def ack_command(cmd: dict, exec_result: dict) -> bool:
    cid = cmd.get("id")
    p = cmd.get("payload") or {}

    # Normalize status into Bus-friendly set
    status_raw = (exec_result.get("status") or "").lower()
    if status_raw == "ok":
        status = "ok"
    elif status_raw == "held":
        status = "skipped"  # treat "held" as non-error skip
    else:
        status = "error"

    detail = {
        "venue":   exec_result.get("venue")  or p.get("venue"),
        "symbol":  exec_result.get("symbol") or p.get("symbol") or p.get("product_id"),
        "side":    exec_result.get("side")   or p.get("side")   or (p.get("action") or "").upper(),
        "fills":   exec_result.get("fills", []),
        "executed_qty": exec_result.get("executed_qty"),
        "avg_price":    exec_result.get("avg_price"),
        "mode": EDGE_MODE,
        "note": exec_result.get("message", ""),
        "ts": int(time.time()),
    }

    body = {"agent_id": AGENT_ID, "command_id": cid, "status": status, "detail": detail}
    try:
        r = _post_json("/api/commands/ack", body, secret=OUTBOX_SECRET, timeout=20)
        _log(f"ack {cid} {r.status_code} {r.text[:160]}")
        return r.ok
    except Exception as e:
        _log(f"ack error {cid}: {e}")
        return False

# --- Pull + loop -------------------------------------------------------------
def pull_once() -> list:
    body = {"agent_id": AGENT_ID, "max": MAX_PULL, "lease_seconds": LEASE_SECONDS}
    r = _post_json("/api/commands/pull", body, secret=OUTBOX_SECRET, timeout=20)
    if r.status_code >= 400:
        raise requests.HTTPError(f"{r.status_code} {r.text[:200]}", response=r)
    try:
        j = r.json()
    except Exception:
        return []
    return j.get("commands", []) if isinstance(j, dict) else []

RECENT_IDS = collections.deque(maxlen=256)
def _seen_before(cid) -> bool:
    s = str(cid)
    if s in RECENT_IDS:
        return True
    RECENT_IDS.append(s)
    return False

_last_hb = 0.0
_last_bal_snap = 0.0

def maybe_heartbeat():
    global _last_hb
    if HEARTBEAT_SECS <= 0: return
    now = time.time()
    if now - _last_hb < HEARTBEAT_SECS: return
    _last_hb = now
    if telemetry_db:
        try:
            telemetry_db.log_heartbeat(agent=AGENT_ID, ok=True, latency_ms=0)
            if TELEMETRY_VERBOSE: _log("telemetry_db: heartbeat row inserted")
        except Exception as e:
            _log(f"telemetry_db heartbeat error: {e}")
    if telemetry_sync:
        try:
            hb = telemetry_sync.send_heartbeat(latency_ms=0)
            if TELEMETRY_VERBOSE: _log(f"heartbeat push: {hb}")
            tp = telemetry_sync.push_telemetry()
            if TELEMETRY_VERBOSE: _log(f"telemetry push: {tp}")
        except Exception as e:
            _log(f"heartbeat/telemetry push error: {e}")

def _printable_balances(bals: dict, wanted: tuple[str, ...], kraken_alias: bool = False) -> dict:
    out = {}
    for k in wanted:
        val = None
        if kraken_alias and k == "BTC":
            val = bals.get("BTC") if "BTC" in bals else bals.get("XBT")
        else:
            val = bals.get(k)
        if val is not None:
            try: out[k] = round(float(val), 8)
            except Exception: out[k] = val
    return out

def maybe_balance_snapshot():
    global _last_bal_snap
    now = time.time()
    if now - _last_bal_snap < BALANCE_SNAPSHOT_SECS:
        return
    _last_bal_snap = now

    def _capture(label, bals):
        try:
            if not isinstance(bals, dict): return
            if telemetry_db:
                telemetry_db.upsert_balances(label, bals)
            wanted = ("USDT","USDC","BTC","XBT")
            view = _printable_balances(bals, wanted, kraken_alias=(label=="KRAKEN"))
            if view: _log(f"snapshot {label} balances: {view}")
        except Exception as e:
            _log(f"snapshot {label} error: {e}")

    try:
        cb = CoinbaseCDP(); _capture("COINBASE", cb.balances())
    except Exception as e:
        _log(f"snapshot COINBASE error: {e}")
    try:
        bus = BinanceUS()
        acct = bus.account()
        bals_bus = { (b.get('asset') or '').upper(): float(b.get('free') or 0.0) for b in (acct.get('balances') or []) }
        _capture("BINANCEUS", bals_bus)
    except Exception as e:
        _log(f"snapshot BINANCEUS error: {e}")
    try:
        _capture("KRAKEN", kraken_balance())
    except Exception as e:
        _log(f"snapshot KRAKEN error: {e}")

# --- Main loop ---------------------------------------------------------------
def main():
    _log(f"online — mode={EDGE_MODE} hold={EDGE_HOLD} base={BASE_URL} agent={AGENT_ID}")
    backoff = 2
    while True:
        try:
            if EDGE_HOLD:
                time.sleep(EDGE_POLL_SECS)
                continue

            cmds = pull_once()
            if cmds:
                _log(f"received {len(cmds)} command(s)")

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
                    res = {"status":"error","message":str(e),"fills":[]}

                try:
                    ok = ack_command(cmd, res)
                except Exception as e:
                    _log(f"ack exception {cid}: {e}")
                    ok = False

                # Optional: you can mirror receipts elsewhere here if desired (left out by default)

            maybe_heartbeat()
            maybe_balance_snapshot()
            maybe_push_balances()
            backoff = 2

        except requests.HTTPError as he:
            try:
                code = he.response.status_code; txt = he.response.text[:200]
            except Exception:
                code, txt = -1, str(he)
            _log(f"poll HTTP {code}: {txt}")
            backoff = min(backoff * 2, 30)
        except Exception as e:
            _log(f"poll error: {e}")
            backoff = min(backoff * 2, 30)
        time.sleep(EDGE_POLL_SECS if backoff <= 2 else backoff)

if __name__ == "__main__":
    main()
