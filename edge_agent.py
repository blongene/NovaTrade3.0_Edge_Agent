#!/usr/bin/env python3
# edge_agent.py — NovaTrade Edge Agent (Phase 6, production-ready)
# Roles:
# - Poll commands from Bus (/api/commands/pull)
# - Execute intents on Coinbase Advanced, BinanceUS, Kraken
# - Support BUY, SELL, and SWAP (from_asset -> quote) with pair resolution + fallback bridges
# - ACK results to /api/commands/ack (DONE/ERROR/HELD) with HMAC
# - Optionally mirror receipts to /api/receipts/ack (if HMAC present)
# - Heartbeat + periodic balance snapshots
# - Optional telemetry push of balances to Bus (/api/telemetry/push_balances)
#
# Assumptions:
# - Venue executors expose `execute_market_order(venue_symbol, side, amount_quote, amount_base, client_id, edge_mode, edge_hold)`
# - Coinbase executor provides CoinbaseCDP().balances()
# - BinanceUS executor provides BinanceUS().account() -> {'balances': [{'asset':'USDT','free':'...'}, ...]}
# - Kraken executor exposes _balance() returning dict of assets (BTC/XBT nuances handled below)
#
# Env (typical):
#   CLOUD_BASE_URL=https://novatrade3-0.onrender.com
#   EDGE_AGENT_ID=edge-primary
#   EDGE_MODE=live|dryrun
#   EDGE_HOLD=false
#   EDGE_SECRET=<shared hmac>
#   EDGE_POLL_SECS=10
#   HEARTBEAT_SECS=900
#   BALANCE_SNAPSHOT_SECS=7200
#   TELEMETRY_VERBOSE=0
#   # Optional telemetry push to Bus Wallet_Monitor writer
#   PUSH_BALANCES_ENABLED=1
#   PUSH_BALANCES_EVERY_S=600
#   WALLET_MONITOR_ENDPOINT=/api/telemetry/push_balances
#
# Notes:
# - SWAP intent schema expected from Bus:
#     {"action":"SWAP","venue":"BINANCEUS","from":"ALT","to":"USDT","amount_usd":75.0,...}
#   Edge computes needed base qty using live price, sells `from` into `to` on that venue.
#   If no direct pair, it tries a simple two-hop bridge (USDT/USDC).

import os, time, json, hmac, hashlib, requests, sys, traceback, re, collections, math

# --- Venue executors ---------------------------------------------------------
from executors.coinbase_advanced_executor import execute_market_order as cb_exec
from executors.binance_us_executor import execute_market_order as bus_exec
from executors.kraken_executor import execute_market_order as kr_exec
from executors.coinbase_advanced_executor import CoinbaseCDP
from executors.binance_us_executor import BinanceUS
from executors.kraken_executor import _balance as kraken_balance

# --- Optional telemetry stores ------------------------------------------------
try:
    import telemetry_db
except Exception:
    telemetry_db = None

try:
    import telemetry_sync
except Exception:
    telemetry_sync = None

# --- Env ---------------------------------------------------------------------
CLOUD_BASE_URL = (os.getenv("CLOUD_BASE_URL") or os.getenv("BASE_URL") or "http://localhost:10000").rstrip("/")
BASE_URL = CLOUD_BASE_URL
AGENT_ID       = (os.getenv("AGENT_ID") or os.getenv("EDGE_AGENT_ID") or "edge-primary").split(",")[0].strip()
EDGE_MODE      = (os.getenv("EDGE_MODE") or "dryrun").strip().lower()   # dryrun | live
EDGE_HOLD      = (os.getenv("EDGE_HOLD") or "false").strip().lower() in {"1","true","yes"}
EDGE_SECRET    = (os.getenv("EDGE_SECRET") or os.getenv("OUTBOX_SECRET") or "").strip()
EDGE_POLL_SECS = int(os.getenv("EDGE_POLL_SECS") or "10")

# Heartbeat / telemetry cadence
HEARTBEAT_SECS = int(os.getenv("HEARTBEAT_SECS") or os.getenv("HEARTBEAT_EVERY_S") or "900")  # default 15m
BALANCE_SNAPSHOT_SECS = int(os.getenv("BALANCE_SNAPSHOT_SECS", "7200"))  # every 2h
TELEMETRY_VERBOSE = (os.getenv("TELEMETRY_VERBOSE") or "0").strip().lower() in {"1","true","yes"}

# Push balances to Bus → Wallet_Monitor
PUSH_BALANCES_ENABLED = (os.getenv("PUSH_BALANCES_ENABLED") or "1").strip().lower() in {"1","true","yes"}
PUSH_BALANCES_EVERY_S = int(os.getenv("PUSH_BALANCES_EVERY_S") or "600")
WALLET_MONITOR_ENDPOINT = (os.getenv("WALLET_MONITOR_ENDPOINT") or "/api/telemetry/push_balances").strip()

# --- HTTP session with retry -------------------------------------------------
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "NovaTrade-Edge/1.6"})
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
_retry = Retry(total=3, connect=3, read=3, backoff_factor=0.5,
               status_forcelist=(429, 502, 503, 504),
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

# --- Logging -----------------------------------------------------------------
def _log(msg: str):
    ts = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
    print(f"[edge] {ts} {msg}", flush=True)

# --- HMAC / signed HTTP ------------------------------------------------------
def _post_signed(url: str, body: dict, timeout=20):
    raw = json.dumps(body, separators=(",", ":"), sort_keys=False).encode("utf-8")
    headers = {"Content-Type": "application/json", "User-Agent": "NovaTrade-Edge/1.6"}
    if EDGE_SECRET:
        sig = hmac.new(EDGE_SECRET.encode("utf-8"), raw, hashlib.sha256).hexdigest()
        headers["X-Outbox-Signature"] = f"sha256={sig}"
    return SESSION.post(url, data=raw, headers=headers, timeout=timeout)

def _post_signed_retry(url: str, body: dict, timeout=20, retries=3):
    for i in range(retries):
        try:
            r = _post_signed(url, body, timeout=timeout)
            if r.status_code < 500:
                return r
        except Exception:
            pass
        time.sleep(2 * (i + 1))
    return _post_signed(url, body, timeout=timeout)

# --- Helpers -----------------------------------------------------------------
def _venue_key(v: str) -> str:
    return re.sub(r"[^A-Z]", "", (v or "").upper())

def _canon(d: dict, sort_keys=True) -> bytes:
    return json.dumps(d, separators=(",", ":"), sort_keys=sort_keys).encode("utf-8")

def _hmac_hex(secret: str, raw_bytes: bytes) -> str:
    return hmac.new(secret.encode("utf-8"), raw_bytes, hashlib.sha256).hexdigest()

# Pair normalizers per venue
def _kr_asset(a: str) -> str:
    a = (a or "").upper()
    return "XBT" if a == "BTC" else a

def _cb_prod(base: str, quote: str) -> str:
    return f"{base.upper()}-{quote.upper()}"

def _bus_sym(base: str, quote: str) -> str:
    return f"{base.upper()}{quote.upper()}"

def _kr_sym(base: str, quote: str) -> str:
    # Kraken often accepts simple concatenation for spot symbols via API adapters
    base2 = _kr_asset(base)
    quote2 = _kr_asset(quote)
    return f"{base2}{quote2}"

def resolve_symbol(venue_key: str, base: str, quote: str) -> str:
    venue_key = _venue_key(venue_key)
    if venue_key in ("COINBASE","COINBASEADV","CBADV"):
        return _cb_prod(base, quote)
    if venue_key in ("BINANCEUS","BUSA"):
        return _bus_sym(base, quote)
    if venue_key == "KRAKEN":
        return _kr_sym(base, quote)
    return f"{base.upper()}-{quote.upper()}"

# --- Public price fetchers (for SWAP sizing) ---------------------------------
def fetch_price(venue_key: str, base: str, quote: str) -> float:
    """Return last price for base/quote, or NaN on failure."""
    v = _venue_key(venue_key)
    try:
        if v in ("BINANCEUS","BUSA"):
            sym = _bus_sym(base, quote)
            r = SESSION.get(f"https://api.binance.us/api/v3/ticker/price", params={"symbol": sym}, timeout=6)
            j = r.json()
            return float(j["price"])
        if v in ("COINBASE","COINBASEADV","CBADV"):
            prod = _cb_prod(base, quote)
            r = SESSION.get(f"https://api.exchange.coinbase.com/products/{prod}/ticker", timeout=6)
            j = r.json()
            return float(j["price"])
        if v == "KRAKEN":
            base2, quote2 = _kr_asset(base), _kr_asset(quote)
            pair = f"{base2}{quote2}"
            r = SESSION.get("https://api.kraken.com/0/public/Ticker", params={"pair": pair}, timeout=6)
            j = r.json()
            # Extract first result
            if j.get("error"):
                raise RuntimeError(",".join(j["error"]))
            res = next(iter(j["result"].values()))
            return float(res["c"][0])
    except Exception as e:
        _log(f"price fetch failed {venue_key} {base}/{quote}: {e}")
    return float("nan")

# --- Balance collectors (also used for telemetry push) -----------------------
def get_balances() -> dict:
    out = {}
    # Coinbase
    try:
        cb = CoinbaseCDP()
        out["COINBASE"] = cb.balances()
    except Exception as e:
        _log(f"balances COINBASE error: {e}")
    # BinanceUS
    try:
        bus = BinanceUS()
        acct = bus.account()
        out["BINANCEUS"] = { (b.get('asset') or '').upper(): float(b.get('free') or 0.0)
                             for b in (acct.get('balances') or []) }
    except Exception as e:
        _log(f"balances BINANCEUS error: {e}")
    # Kraken
    try:
        out["KRAKEN"] = kraken_balance()
    except Exception as e:
        _log(f"balances KRAKEN error: {e}")
    return out

def maybe_push_balances():
    """Push balances to Bus Wallet_Monitor writer endpoint."""
    if not PUSH_BALANCES_ENABLED:
        return
    now = time.time()
    if not hasattr(maybe_push_balances, "_last"):
        maybe_push_balances._last = 0.0
    if now - maybe_push_balances._last < max(60, PUSH_BALANCES_EVERY_S):
        return
    maybe_push_balances._last = now
    try:
        payload = {"agent": f"NovaTrade-Edge/{AGENT_ID}", "balances": get_balances()}
        url = f"{BASE_URL}{WALLET_MONITOR_ENDPOINT}"
        r = _post_signed(url, payload, timeout=15)
        _log(f"push_balances {r.status_code} {r.text[:120]}")
    except Exception as e:
        _log(f"push_balances error: {e}")

# --- Payload normalization ----------------------------------------------------
def _normalize_payload(p: dict):
    """Return dict with normalized keys for execution."""
    venue_key = _venue_key(p.get("venue"))
    action    = (p.get("action") or p.get("side") or "").upper()
    side      = (p.get("side") or "").upper()
    symbol    = p.get("symbol") or p.get("product_id") or ""

    # amounts
    amount_quote = None
    for k in ("amount_quote", "amount_usd", "quote_amount", "amount"):
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

# --- SWAP pathfinder ---------------------------------------------------------
def _direct_or_bridge_paths(venue: str, from_asset: str, to_quote: str):
    """
    Yield (base, quote) hops to achieve from_asset -> to_quote.
    Prefer direct. If not, try simple bridges via USDT/USDC.
    """
    v = _venue_key(venue)
    from_a = from_asset.upper(); to_q = to_quote.upper()
    if from_a == to_q:
        return [(from_a, to_q)]
    candidates = []
    # 1) direct
    candidates.append([(from_a, to_q)])
    # 2) bridge via USDT
    if to_q != "USDT":
        candidates.append([(from_a, "USDT"), ("USDT", to_q)])
    # 3) bridge via USDC
    if to_q != "USDC":
        candidates.append([(from_a, "USDC"), ("USDC", to_q)])
    # 4) allow USDT<->USDC hop
    if to_q in ("USDT","USDC"):
        other = "USDC" if to_q == "USDT" else "USDT"
        candidates.append([(from_a, other), (other, to_q)])
    # Return as list of lists
    return candidates

def _estimate_base_qty(venue: str, base: str, quote: str, want_quote_amount: float) -> float:
    px = fetch_price(venue, base, quote)
    if not (px and px == px and px > 0):  # NaN check
        raise RuntimeError(f"no price for {venue} {base}/{quote}")
    # add tiny safety margin to ensure enough is sold to net desired quote after fees/slippage
    safety = 1.0025
    return (want_quote_amount / px) * safety

# --- Core execution -----------------------------------------------------------
def execute_market(venue_key: str, base: str, quote: str, side: str, amount_quote: float = 0.0, amount_base: float = 0.0, client_id: str = ""):
    """Route to venue executor with normalized symbol."""
    symbol = resolve_symbol(venue_key, base, quote)
    exe = EXECUTORS.get(_venue_key(venue_key))
    if not exe:
        raise RuntimeError(f"executor missing for {venue_key}")
    return exe(venue_symbol=symbol, side=side, amount_quote=amount_quote, amount_base=amount_base, client_id=str(client_id), edge_mode=EDGE_MODE, edge_hold=EDGE_HOLD)

def handle_swap(cmd: dict, pnorm: dict) -> dict:
    venue = pnorm["venue"]
    from_asset = pnorm["from"] or (pnorm["symbol"].split("-")[0] if pnorm["symbol"] else "")
    to_quote   = pnorm["to"] or pnorm["target_quote"] or "USDT"
    want_q     = max(0.0, float(pnorm["amount_quote"]))

    if not (venue and from_asset and to_quote and want_q > 0):
        return {"status":"error","message":"SWAP requires venue, from, to, amount_quote>0","fills":[]}

    if from_asset == to_quote:
        return {"status":"ok","message":"SWAP no-op: from==to","fills":[],"venue":venue,"symbol":resolve_symbol(venue, from_asset, to_quote),"side":"SELL","executed_qty":0.0,"avg_price":0.0}

    # Try direct then bridges
    paths = _direct_or_bridge_paths(venue, from_asset, to_quote)
    all_fills = []

    for hop_idx, path in enumerate(paths):
        try:
            # --- Path 1: Direct SELL ---
            if len(path) == 1:
                (base, quote) = path[0]
                if base != from_asset or quote != to_quote: continue

                qty_base = _estimate_base_qty(venue, base, quote, want_q)
                res = execute_market(venue, base, quote, side="SELL", amount_base=qty_base, amount_quote=0.0, client_id=cmd.get("id"))

                if (res.get("status") or "") != "ok":
                    raise RuntimeError(f"Direct path execution failed: {res.get('message', 'unknown error')}")

                # Merge executor result with standard SWAP response fields
                out = res.copy()
                out.update({"status":"ok", "message": f"SWAP direct path executed", "fills": res.get("fills", [])})
                return out

            # --- Path 2: Two-hop bridge SELL -> SELL ---
            elif len(path) == 2:
                (base1, quote1), (base2, quote2) = path
                bridge_asset = quote1
                if quote1 != base2: continue

                # Estimate how much bridge asset we need, then how much from_asset to sell to get it
                qty_bridge_needed = _estimate_base_qty(venue, base2, quote2, want_q)
                qty_from_asset_to_sell = _estimate_base_qty(venue, base1, quote1, qty_bridge_needed)

                # --- EXECUTION ---
                # Leg 1: Sell from_asset for bridge_asset
                res1 = execute_market(venue, base1, quote1, side="SELL", amount_base=qty_from_asset_to_sell, amount_quote=0.0, client_id=cmd.get("id"))
                if (res1.get("status") or "") != "ok":
                    raise RuntimeError(f"Bridge leg 1 ({base1}->{quote1}) failed: {res1.get('message', 'unknown error')}")

                # Determine actual amount of bridge asset received
                received_bridge_qty = float(res1.get("executed_qty", 0.0)) * float(res1.get("avg_price", 0.0))
                if received_bridge_qty <= 0:
                     raise RuntimeError(f"Bridge leg 1 resulted in zero {bridge_asset}")
                all_fills.extend(res1.get("fills", []))

                # Leg 2: Sell the received bridge_asset for the final to_quote asset
                res2 = execute_market(venue, base2, quote2, side="SELL", amount_base=received_bridge_qty, amount_quote=0.0, client_id=cmd.get("id"))
                if (res2.get("status") or "") != "ok":
                    raise RuntimeError(f"Bridge leg 2 ({base2}->{quote2}) failed: {res2.get('message', 'unknown error')}")
                all_fills.extend(res2.get("fills", []))

                # Success
                symbol_final = resolve_symbol(venue, from_asset, to_quote)
                return {"status":"ok", "message": f"SWAP bridge via {bridge_asset} ok", "fills": all_fills, "venue": venue, "symbol": symbol_final, "side": "SELL"}

            # else: paths with <1 or >2 hops are not supported by this logic
        except Exception as e:
            _log(f"SWAP path failed {venue} {path}: {e}")
            all_fills = [] # Reset for next path attempt
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

    # BUY / SELL (legacy)
    if not side:
        side = "BUY" if pnorm["amount_quote"] > 0 else "SELL"
    # Guardrails
    if side == "BUY" and pnorm["amount_quote"] <= 0:
        return {"status":"error","message":"BUY requires amount_quote > 0","fills":[],"venue":venue,"symbol":symbol,"side":side}
    if side == "SELL" and pnorm["amount_base"] <= 0:
        return {"status":"error","message":"SELL requires base_amount > 0","fills":[],"venue":venue,"symbol":symbol,"side":side}

    # Pass through
    try:
        res = EXECUTORS[_venue_key(venue)](
            venue_symbol=symbol, side=side,
            amount_quote=pnorm["amount_quote"],
            amount_base=pnorm["amount_base"],
            client_id=str(cmd.get("id")), edge_mode=EDGE_MODE, edge_hold=EDGE_HOLD
        )
    except Exception as e:
        return {"status":"error","message":str(e),"fills":[],"venue":venue,"symbol":symbol,"side":side}

    res.setdefault("venue", venue)
    res.setdefault("symbol", symbol)
    res.setdefault("side", side)
    return res

# --- ACK & receipts -----------------------------------------------------------
def ack_command(cmd: dict, exec_result: dict) -> bool:
    cmd_id = cmd.get("id")
    p = cmd.get("payload") or {}
    venue  = (exec_result.get("venue")  or p.get("venue")  or "").upper()
    symbol =  exec_result.get("symbol") or p.get("symbol") or p.get("product_id") or ""
    side   = (exec_result.get("side")   or p.get("side")   or "").upper()

    amount_quote = float(p.get("amount_quote") or p.get("amount_usd") or p.get("quote_amount") or p.get("amount") or 0.0)
    amount_base  = float(p.get("base_amount") or 0.0)

    status = (exec_result.get("status") or "").lower()
    ack_status = "DONE" if status == "ok" else ("HELD" if status == "held" else "ERROR")

    body = {
        "id": cmd_id,
        "status": ack_status,
        "receipt": {
            "venue": venue,
            "symbol": symbol,
            "side": side or (p.get("action") or "").upper(),
            "amount_quote": amount_quote,
            "amount_base": amount_base,
            "txid": exec_result.get("txid", ""),
            "fills": exec_result.get("fills", []),
            "executed_qty": exec_result.get("executed_qty"),
            "avg_price": exec_result.get("avg_price"),
            "ts": int(time.time()),
            "agent": AGENT_ID,
            "mode": EDGE_MODE,
            "note": exec_result.get("message",""),
        },
    }
    url = f"{BASE_URL.rstrip('/')}/api/commands/ack"
    r = _post_signed_retry(url, body, timeout=20, retries=3)
    _log(f"ack {cmd_id} {r.status_code} {r.text[:160]}")
    return r.ok

def post_receipt(cmd: dict, exec_result: dict):
    if not EDGE_SECRET:
        return
    payload = {
        "id": cmd["id"],
        "agent_id": AGENT_ID,
        "venue": (cmd.get("payload") or {}).get("venue") or exec_result.get("venue"),
        "symbol": (cmd.get("payload") or {}).get("symbol") or exec_result.get("symbol"),
        "side": (cmd.get("payload") or {}).get("side") or exec_result.get("side"),
        "status": "ok" if (exec_result.get("status") or "") == "ok" else exec_result.get("status", "error"),
        "txid": exec_result.get("txid", ""),
        "fills": exec_result.get("fills", []),
        "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "note": exec_result.get("message", ""),
        "requested_symbol": exec_result.get("requested_symbol"),
        "resolved_symbol":  exec_result.get("resolved_symbol") or exec_result.get("symbol"),
        "post_balances":    exec_result.get("post_balances"),
    }
    canon = _canon({k: v for k, v in payload.items() if k != "hmac"})
    payload["hmac"] = _hmac_hex(EDGE_SECRET, canon)

    url = f"{CLOUD_BASE_URL}/api/receipts/ack"
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

# --- Pull + loop --------------------------------------------------------------
def _parse_pull_response(r):
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
    url  = f"{CLOUD_BASE_URL}/api/commands/pull"
    body = {"agent_id": AGENT_ID, "limit": limit}
    r = _post_signed(url, body, timeout=20)
    if r.status_code >= 400:
        raise requests.HTTPError(f"{r.status_code} {r.text[:200]}", response=r)
    return _parse_pull_response(r)

RECENT_IDS = collections.deque(maxlen=256)
def _seen_before(cid):
    s = str(cid)
    if s in RECENT_IDS:
        return True
    RECENT_IDS.append(s)
    return False

_last_hb = 0
_last_bal_snap = 0

def maybe_heartbeat():
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

def _printable_balances(bals: dict, wanted: tuple[str, ...], kraken_alias: bool = False) -> dict:
    out = {}
    for k in wanted:
        val = None
        if kraken_alias and k == "BTC":
            val = bals.get("BTC", None)
            if val is None:
                val = bals.get("XBT", None)
        else:
            val = bals.get(k, None)
        if val is not None:
            try:
                out[k] = round(float(val), 8)
            except Exception:
                out[k] = val
    return out

def maybe_balance_snapshot():
    global _last_bal_snap
    now = time.time()
    if now - _last_bal_snap < BALANCE_SNAPSHOT_SECS:
        return
    _last_bal_snap = now

    def _capture(label, bals):
        try:
            if not isinstance(bals, dict):
                return
            if telemetry_db:
                telemetry_db.upsert_balances(label, bals)
            wanted = ("USDT", "USDC", "BTC", "XBT")
            view = _printable_balances(bals, tuple(wanted), kraken_alias=(label == "KRAKEN"))
            if view:
                _log(f"snapshot {label} balances: {view}")
        except Exception as e:
            _log(f"snapshot {label} error: {e}")

    # Coinbase
    try:
        cb = CoinbaseCDP()
        _capture("COINBASE", cb.balances())
    except Exception as e:
        _log(f"snapshot COINBASE error: {e}")
    # BinanceUS
    try:
        bus = BinanceUS()
        acct = bus.account()
        bals_bus = { (b.get('asset') or '').upper(): float(b.get('free') or 0.0)
                     for b in (acct.get('balances') or []) }
        _capture("BINANCEUS", bals_bus)
    except Exception as e:
        _log(f"snapshot BINANCEUS error: {e}")
    # Kraken
    try:
        _capture("KRAKEN", kraken_balance())
    except Exception as e:
        _log(f"snapshot KRAKEN error: {e}")

# --- Main loop ----------------------------------------------------------------
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

                # ACK
                ok = False
                try:
                    ok = ack_command(cmd, res)
                except Exception as e:
                    _log(f"ack exception {cid}: {e}")

                # Optional receipt mirror
                if ok:
                    try:
                        post_receipt(cmd, res)
                    except Exception as e:
                        _log(f"post_receipt exception {cid}: {e}")

            maybe_heartbeat()
            maybe_balance_snapshot()
            maybe_push_balances()

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

from edge_bus_poller import start_bus_poller
start_bus_poller()
# … your existing balance push loop continues as-is
