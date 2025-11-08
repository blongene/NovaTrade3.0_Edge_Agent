#!/usr/bin/env python3
"""
NovaTrade 3.0 — Edge Agent (drop‑in)
* Safe pull→execute→ack loop with HMAC signing
* Works with BTCUSDT / BTC-USDT symbols (auto‑parser)
* Policy/hold aware; dry or live via EDGE_MODE
* Heartbeat + optional balance snapshots / push

Env (typical):
  BASE_URL=https://novatrade3-0.onrender.com
  AGENT_ID=edge-primary
  EDGE_SECRET=...           # same as Bus OUTBOX_SECRET
  EDGE_MODE=live|dry        # default dry
  EDGE_HOLD=false|true      # default false
  EDGE_POLL_SECS=8          # wait between polls when healthy
  LEASE_SECONDS=120
  MAX_CMDS_PER_PULL=5
  BALANCE_SNAPSHOT_SECS=7200
  PUSH_BALANCES_ENABLED=1
  PUSH_BALANCES_EVERY_S=600
"""

from __future__ import annotations
import os, time, json, hmac, hashlib, re, traceback, pathlib, collections
from typing import Dict, Any, Optional, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ------- HTTP session with retry -------
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "NovaTrade-Edge/3.0"})
_retry = Retry(total=3, connect=3, read=3, backoff_factor=0.5,
               status_forcelist=(429, 500, 502, 503, 504),
               allowed_methods=frozenset(["GET", "POST"]))
SESSION.mount("https://", HTTPAdapter(max_retries=_retry))
SESSION.mount("http://",  HTTPAdapter(max_retries=_retry))

# ------- Env / config -------
BASE_URL   = (os.getenv("CLOUD_BASE_URL") or os.getenv("BASE_URL") or "http://localhost:10000").rstrip("/")
AGENT_ID   = (os.getenv("AGENT_ID") or os.getenv("EDGE_AGENT_ID") or "edge-primary").split(",")[0].strip()
EDGE_MODE  = (os.getenv("EDGE_MODE") or "dry").strip().lower()   # live|dry
EDGE_HOLD  = (os.getenv("EDGE_HOLD") or "false").strip().lower() in {"1","true","yes"}

OUTBOX_SECRET    = (os.getenv("EDGE_SECRET") or os.getenv("OUTBOX_SECRET") or os.getenv("BUS_SECRET") or "").strip()
TELEMETRY_SECRET = (os.getenv("TELEMETRY_SECRET") or OUTBOX_SECRET).strip()

EDGE_POLL_SECS = int(os.getenv("EDGE_POLL_SECS") or os.getenv("POLL_SEC") or "8")
LEASE_SECONDS  = int(os.getenv("LEASE_SECONDS") or "120")
MAX_PULL       = int(os.getenv("MAX_CMDS_PER_PULL") or "5")

HEARTBEAT_SECS        = int(os.getenv("HEARTBEAT_SECS") or "900")
BALANCE_SNAPSHOT_SECS = int(os.getenv("BALANCE_SNAPSHOT_SECS") or "7200")
PUSH_BALANCES_ENABLED = (os.getenv("PUSH_BALANCES_ENABLED") or "1").lower() in {"1","true","yes"}
PUSH_BALANCES_EVERY_S = int(os.getenv("PUSH_BALANCES_EVERY_S") or "600")

# ------- Light logger -------

def _log(msg: str):
    ts = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
    print(f"[edge] {ts} {msg}", flush=True)

# ------- HMAC helpers -------

def _canon(body: Dict[str, Any]) -> bytes:
    return json.dumps(body, separators=(",", ":"), sort_keys=True).encode()

def _sig(secret: str, body: Dict[str, Any]) -> str:
    return hmac.new(secret.encode(), _canon(body), hashlib.sha256).hexdigest() if secret else ""

# Bus POST (header MUST be X-Nova-Signature)

def bus_post(path: str, body: Dict[str, Any], timeout: int = 20) -> requests.Response:
    headers = {"Content-Type": "application/json"}
    if OUTBOX_SECRET:
        headers["X-Nova-Signature"] = _sig(OUTBOX_SECRET, body)
    return SESSION.post(f"{BASE_URL}{path}", json=body, headers=headers, timeout=timeout)

# =============== Symbol helpers ===============
STABLES = ("USDT", "USDC", "USD")


def parse_symbol(s: str) -> Tuple[str, str]:
    """Return (base, quote) from BTC-USDT or BTCUSDT style; fallback quote USD."""
    s = (s or "").upper()
    if "-" in s:
        b, q = (s.split("-", 1) + [""])[:2]
        return b, q
    m = re.match(r"^([A-Z0-9]+?)(USDT|USDC|USD)$", s)
    if m:
        return m.group(1), m.group(2)
    return s, "USD"

# =============== Venue executors ===============
# Import your real executors. These should accept the call signature below.
try:
    from executors.coinbase_advanced_executor import execute_market_order as cb_exec, CoinbaseCDP
except Exception:
    cb_exec = None
    CoinbaseCDP = None  # type: ignore

try:
    from executors.binance_us_executor import execute_market_order as bus_exec, BinanceUS
except Exception:
    bus_exec = None
    BinanceUS = None  # type: ignore

try:
    from executors.kraken_executor import execute_market_order as kr_exec, _balance as kraken_balance
except Exception:
    kr_exec = None
    def kraken_balance():
        return {}

EXECUTORS = {
    "COINBASE":    cb_exec,
    "COINBASEADV": cb_exec,
    "CBADV":       cb_exec,
    "BINANCEUS":   bus_exec,
    "BUSA":        bus_exec,
    "KRAKEN":      kr_exec,
}

# Policy gate (best‑effort):
try:
    from edge_pretrade import pretrade_validate
except Exception:
    def pretrade_validate(**kwargs):  # type: ignore
        # allow all if policy module missing
        return True, "ok", kwargs.get("quote"), 0.0, 0.0

# =============== Balances / telemetry (optional) ===============
try:
    import telemetry_db
except Exception:
    telemetry_db = None  # type: ignore

try:
    import telemetry_sync
except Exception:
    telemetry_sync = None  # type: ignore


def get_balances() -> dict:
    out: dict[str, dict] = {}
    try:
        if CoinbaseCDP:
            out["COINBASE"] = CoinbaseCDP().balances()
    except Exception as e:
        _log(f"balances COINBASE error: {e}")
    try:
        if BinanceUS:
            acct = BinanceUS().account()
            out["BINANCEUS"] = { (b.get("asset") or "").upper(): float(b.get("free") or 0.0)
                                   for b in (acct.get("balances") or []) }
    except Exception as e:
        _log(f"balances BINANCEUS error: {e}")
    try:
        out["KRAKEN"] = kraken_balance() or {}
    except Exception as e:
        _log(f"balances KRAKEN error: {e}")
    return out


_last_hb = 0.0
_last_bal_snap = 0.0
_last_push_bal = 0.0


def maybe_heartbeat():
    global _last_hb
    if HEARTBEAT_SECS <= 0: return
    now = time.time()
    if now - _last_hb < HEARTBEAT_SECS: return
    _last_hb = now
    if telemetry_db:
        try:
            telemetry_db.log_heartbeat(agent=AGENT_ID, ok=True, latency_ms=0)
        except Exception as e:
            _log(f"telemetry_db heartbeat error: {e}")
    if telemetry_sync and hasattr(telemetry_sync, "send_heartbeat"):
        try:
            telemetry_sync.send_heartbeat(latency_ms=0)
        except Exception as e:
            _log(f"heartbeat push error: {e}")


def _printable_balances(bals: dict, wanted: tuple[str, ...]) -> dict:
    out = {}
    for k in wanted:
        if k in bals:
            try: out[k] = round(float(bals[k]), 8)
            except Exception: out[k] = bals[k]
    return out


def maybe_balance_snapshot():
    global _last_bal_snap
    now = time.time()
    if now - _last_bal_snap < BALANCE_SNAPSHOT_SECS: return
    _last_bal_snap = now
    bals = get_balances()
    if not bals: return
    if telemetry_db:
        try:
            for venue, v in bals.items():
                telemetry_db.upsert_balances(venue, v)
        except Exception as e:
            _log(f"telemetry_db balances error: {e}")
    view = {k: _printable_balances(v, ("USDT","USDC","USD","BTC","XBT")) for k,v in bals.items()}
    _log(f"snapshot balances: {view}")


def maybe_push_balances():
    global _last_push_bal
    if not PUSH_BALANCES_ENABLED or telemetry_sync is None: return
    now = time.time()
    if now - _last_push_bal < max(60, PUSH_BALANCES_EVERY_S): return
    _last_push_bal = now
    try:
        if hasattr(telemetry_sync, "push_telemetry"):
            telemetry_sync.push_telemetry()
            _log("balances push: ok")
    except Exception as e:
        _log(f"balances push error: {e}")

# =============== Price fetch (public) ===============

def fetch_price(venue: str, base: str, quote: str) -> float:
    v = re.sub(r"[^A-Z]", "", (venue or "").upper())
    try:
        if v in ("BINANCEUS", "BUSA"):
            sym = f"{base.upper()}{quote.upper()}"
            j = SESSION.get("https://api.binance.us/api/v3/ticker/price", params={"symbol": sym}, timeout=6).json()
            return float(j["price"])
        if v in ("COINBASE", "COINBASEADV", "CBADV"):
            prod = f"{base.upper()}-{quote.upper()}"
            j = SESSION.get(f"https://api.exchange.coinbase.com/products/{prod}/ticker", timeout=6).json()
            return float(j["price"])
        if v == "KRAKEN":
            def _kr(a: str) -> str: return "XBT" if a.upper()=="BTC" else a.upper()
            pair = f"{_kr(base)}{_kr(quote)}"
            j = SESSION.get("https://api.kraken.com/0/public/Ticker", params={"pair": pair}, timeout=6).json()
            if j.get("error"): raise RuntimeError(",".join(j["error"]))
            res = next(iter(j["result"].values()))
            return float(res["c"][0])
    except Exception as e:
        _log(f"price fetch failed {venue} {base}/{quote}: {e}")
    return float("nan")

# =============== Execution core ===============

def _venue_key(v: str) -> str:
    return re.sub(r"[^A-Z]", "", (v or "").upper())


def resolve_symbol(venue_key: str, base: str, quote: str) -> str:
    v = _venue_key(venue_key)
    if v in ("COINBASE", "COINBASEADV", "CBADV"): return f"{base.upper()}-{quote.upper()}"
    if v in ("BINANCEUS", "BUSA"):               return f"{base.upper()}{quote.upper()}"
    if v == "KRAKEN":                               return f"{('XBT' if base.upper()=='BTC' else base.upper())}{('XBT' if quote.upper()=='BTC' else quote.upper())}"
    return f"{base.upper()}-{quote.upper()}"


def normalize_amounts_from_intent(intent: dict, price: float) -> dict:
    amt = float(intent.get("amount", 0) or 0)
    flags = {str(x).lower() for x in intent.get("flags", [])}
    out = {"amount_base": 0.0, "amount_quote": 0.0}
    if "quote" in flags:  # amount is quote currency
        out["amount_quote"] = max(0.0, amt)
        if price and price > 0:
            out["amount_base"] = out["amount_quote"] / float(price)
    else:                   # amount is base currency
        out["amount_base"] = max(0.0, amt)
        if price and price > 0:
            out["amount_quote"] = out["amount_base"] * float(price)
    return out


def execute_market(venue_key: str, base: str, quote: str, side: str,
                   amount_quote: float = 0.0, amount_base: float = 0.0, client_id: str = ""):
    exe = EXECUTORS.get(_venue_key(venue_key))
    if not exe:
        raise RuntimeError(f"executor missing for {venue_key}")
    symbol_for_exec = resolve_symbol(venue_key, base, quote)
    return exe(
        venue_symbol=symbol_for_exec,
        side=side,
        amount_quote=amount_quote,
        amount_base=amount_base,
        client_id=str(client_id),
        edge_mode=EDGE_MODE,
        edge_hold=EDGE_HOLD,
    )


def exec_command(cmd: dict, balances_cache: Optional[dict] = None) -> dict:
    payload = cmd.get("intent") or cmd.get("payload") or {}
    venue  = (payload.get("venue") or "").upper()
    side   = (payload.get("side")  or "").upper()
    symbol = payload.get("symbol") or payload.get("product_id") or ""

    base, quote = parse_symbol(symbol)
    px = fetch_price(venue, base, quote)
    sized = normalize_amounts_from_intent(payload, px)

    # HOLD path
    if EDGE_HOLD:
        return {"status":"held","message":"EDGE_HOLD enabled","fills":[],"venue":venue,"symbol":symbol,"side":side or "?","price_usd":px if px==px else None}

    # infer side if not given
    if not side:
        side = "BUY" if sized["amount_quote"]>0 else ("SELL" if sized["amount_base"]>0 else "")
        if not side:
            return {"status":"error","message":"missing side/amount","fills":[],"venue":venue,"symbol":symbol,"side":side}

    venue_balances = (balances_cache or {}).get(venue) or {}
    ok, reason, chosen_quote, *_ = pretrade_validate(
        venue=venue, base=base, quote=quote, price=px,
        amount_base=sized["amount_base"], amount_quote=sized["amount_quote"],
        venue_balances=venue_balances,
    )
    if not ok:
        return {"status":"error","message":reason,"fills":[],"venue":venue,"symbol":symbol,"side":side}

    if chosen_quote and chosen_quote != quote:
        quote = chosen_quote

    try:
        res = execute_market(
            venue_key=venue, base=base, quote=quote, side=side,
            amount_quote=sized["amount_quote"] if side=="BUY" else 0.0,
            amount_base=sized["amount_base"]   if side=="SELL" else 0.0,
            client_id=str(cmd.get("id")),
        )
    except Exception as e:
        return {"status":"error","message":str(e),"fills":[],"venue":venue,"symbol":symbol,"side":side}

    res.setdefault("venue", venue)
    res.setdefault("symbol", f"{base}-{quote}")
    res.setdefault("side", side)
    if px==px: res.setdefault("price_usd", px)
    return res

# =============== Pull / Ack ===============

_receipts_path = pathlib.Path("receipts.jsonl")
RECENT_IDS = collections.deque(maxlen=256)


def durable_ack(cmd_id: int, ok: bool, receipt: dict):
    # local append first
    try:
        with _receipts_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps({"ts":int(time.time()),"agent_id":AGENT_ID,"cmd_id":cmd_id,"ok":ok,"receipt":receipt})+"\n")
    except Exception as e:
        _log(f"receipts.jsonl append error: {e}")

    body = {"agent_id": AGENT_ID, "cmd_id": cmd_id, "ok": ok, "receipt": receipt}
    backoff = 1
    for _ in range(4):
        try:
            r = bus_post("/api/commands/ack", body, timeout=20)
            _log(f"ack {cmd_id} {r.status_code} {r.text[:160]}")
            if r.ok: return True
        except Exception as e:
            _log(f"ack error {cmd_id}: {e}")
        time.sleep(backoff)
        backoff = min(backoff*2, 30)
    return False


def pull_once() -> list[dict]:
    body = {"agent_id": AGENT_ID, "limit": MAX_PULL, "lease_seconds": LEASE_SECONDS}
    r = bus_post("/api/commands/pull", body, timeout=20)
    if r.status_code >= 400:
        raise requests.HTTPError(f"{r.status_code} {r.text[:200]}", response=r)
    try:
        j = r.json()
        return j.get("commands", []) if isinstance(j, dict) else []
    except Exception:
        return []

# =============== Main loop ===============

def wait_for_bus(max_wait_s: int = 120):
    deadline = time.time() + max_wait_s
    while time.time() < deadline:
        try:
            r = SESSION.get(f"{BASE_URL}/healthz", timeout=5)
            if r.ok:
                return
        except Exception:
            pass
        time.sleep(2)
    _log("warning: bus warm-up timeout; continuing with backoff")


def main():
    _log(f"online — mode={EDGE_MODE} hold={EDGE_HOLD} base={BASE_URL} agent={AGENT_ID}")
    wait_for_bus()
    backoff = 2
    while True:
        try:
            cmds = pull_once()
            if cmds:
                _log(f"received {len(cmds)} command(s)")

            balances_cache = get_balances()

            for cmd in cmds:
                cid = cmd.get("id")
                sid = str(cid)
                if sid in RECENT_IDS:
                    _log(f"skip duplicate id {cid}")
                    continue
                RECENT_IDS.append(sid)

                try:
                    res = exec_command(cmd, balances_cache=balances_cache)
                except Exception as e:
                    traceback.print_exc()
                    res = {"status":"error","message":str(e),"fills":[]}

                ok = (str(res.get("status",""))).lower() == "ok"
                receipt = {
                    "normalized": {
                        "receipt_id": f"edge-{AGENT_ID}-{int(time.time())}",
                        "venue":    res.get("venue") or (cmd.get("intent") or {}).get("venue"),
                        "symbol":   res.get("symbol") or (cmd.get("intent") or {}).get("symbol"),
                        "side":     res.get("side")   or (cmd.get("intent") or {}).get("side"),
                        "executed_qty": res.get("executed_qty"),
                        "avg_price":    res.get("avg_price"),
                        "fee":          res.get("fee"),
                        "fee_asset":    res.get("fee_asset"),
                        "status":   res.get("status"),
                    },
                    "raw": res,
                }
                durable_ack(int(cid), ok, receipt)

            maybe_heartbeat()
            maybe_balance_snapshot()
            maybe_push_balances()
            backoff = 2
        except requests.HTTPError as he:
            code = getattr(getattr(he, "response", None), "status_code", -1)
            txt  = getattr(getattr(he, "response", None), "text", str(he))
            _log(f"poll HTTP {code}: {txt[:200]}")
            backoff = min(backoff*2, 30)
        except Exception as e:
            _log(f"poll error: {e}")
            backoff = min(backoff*2, 30)
        time.sleep(EDGE_POLL_SECS if backoff<=2 else backoff)


if __name__ == "__main__":
    main()

# --- universal drop-in for EdgeAgent ---
def execute_market_order(intent: dict = None):
    """EdgeAgent entrypoint shim."""
    from typing import Dict
    if intent is None:
        return {"status": "noop", "message": "no intent provided"}
    return execute(intent) if "execute" in globals() else {"status": "ok", "message": "simulated exec"}
