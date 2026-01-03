#!/usr/bin/env python3
"""
NovaTrade 3.0 — Edge Agent (BinanceUS + multi-venue executor)

* Safe pull→execute→ack loop with HMAC signing
* Symbol parsing for BTCUSDT / BTC-USDT / BTC/USDT forms
* Policy / hold aware; dry or live via EDGE_MODE
* Heartbeat + optional balance snapshots / push

Env (typical):

  BASE_URL=https://novatrade3-0.onrender.com
  CLOUD_BASE_URL=...                # optional override
  AGENT_ID=edge-primary
  EDGE_SECRET=...                   # same as Bus OUTBOX_SECRET
  EDGE_MODE=live|dry                # default dry
  EDGE_HOLD=false|true              # default false
  EDGE_POLL_SECS=8
  LEASE_SECONDS=120
  MAX_CMDS_PER_PULL=5
  HEARTBEAT_SECS=900
  BALANCE_SNAPSHOT_SECS=7200
  PUSH_BALANCES_ENABLED=1
  PUSH_BALANCES_EVERY_S=600

  BINANCE_API_KEY=...
  BINANCE_API_SECRET=...            # or BINANCE_SECRET_KEY
"""

from __future__ import annotations

import collections
import hashlib
import hmac
import json
import os
import pathlib
import re
import time
import traceback
from typing import Any, Dict, Optional, Tuple, List

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ---------------------------------------------------------------------------
# HTTP session with retry
# ---------------------------------------------------------------------------

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "NovaTrade-Edge/3.0"})

_retry = Retry(
    total=3,
    connect=3,
    read=3,
    backoff_factor=0.5,
    status_forcelist=(429, 500, 502, 503, 504),
    allowed_methods=frozenset(["GET", "POST"]),
)

SESSION.mount("https://", HTTPAdapter(max_retries=_retry))
SESSION.mount("http://", HTTPAdapter(max_retries=_retry))

# ---------------------------------------------------------------------------
# Env / config
# ---------------------------------------------------------------------------

BASE_URL = (
    os.getenv("CLOUD_BASE_URL") or os.getenv("BASE_URL") or "http://localhost:10000"
).rstrip("/")

AGENT_ID = (
    os.getenv("AGENT_ID") or os.getenv("EDGE_AGENT_ID") or "edge-primary"
).split(",")[0].strip()

EDGE_MODE = (os.getenv("EDGE_MODE") or "dry").strip().lower()  # live|dry
EDGE_HOLD = (os.getenv("EDGE_HOLD") or "false").strip().lower() in {"1", "true", "yes"}

OUTBOX_SECRET = (
    os.getenv("EDGE_SECRET") or os.getenv("OUTBOX_SECRET") or os.getenv("BUS_SECRET") or ""
).strip()
TELEMETRY_SECRET = (os.getenv("TELEMETRY_SECRET") or OUTBOX_SECRET).strip()

EDGE_POLL_SECS = int(os.getenv("EDGE_POLL_SECS") or os.getenv("POLL_SEC") or "8")
LEASE_SECONDS = int(os.getenv("LEASE_SECONDS") or "120")
MAX_PULL = int(os.getenv("MAX_CMDS_PER_PULL") or "5")

HEARTBEAT_SECS = int(os.getenv("HEARTBEAT_SECS") or "900")
BALANCE_SNAPSHOT_SECS = int(os.getenv("BALANCE_SNAPSHOT_SECS") or "7200")
PUSH_BALANCES_ENABLED = (os.getenv("PUSH_BALANCES_ENABLED") or "1").lower() in {
    "1",
    "true",
    "yes",
}
PUSH_BALANCES_EVERY_S = int(os.getenv("PUSH_BALANCES_EVERY_S") or "600")

# ---------------------------------------------------------------------------
# Light logger
# ---------------------------------------------------------------------------


def _log(msg: str) -> None:
    ts = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
    print(f"[edge] {ts} {msg}", flush=True)


# ---------------------------------------------------------------------------
# HMAC helpers
# ---------------------------------------------------------------------------


def _canon(body: Dict[str, Any]) -> bytes:
    return json.dumps(body, separators=(",", ":"), sort_keys=True).encode()


def _sig(secret: str, body: Dict[str, Any]) -> str:
    return (
        hmac.new(secret.encode(), _canon(body), hashlib.sha256).hexdigest()
        if secret
        else ""
    )


def bus_post(path: str, body: Dict[str, Any], timeout: int = 20) -> requests.Response:
    """
    POST to Bus with X-Nova-Signature header.
    """
    headers = {"Content-Type": "application/json"}
    if OUTBOX_SECRET:
        headers["X-Nova-Signature"] = _sig(OUTBOX_SECRET, body)
    return SESSION.post(f"{BASE_URL}{path}", json=body, headers=headers, timeout=timeout)


# ---------------------------------------------------------------------------
# Symbol helpers
# ---------------------------------------------------------------------------

STABLES = ("USDT", "USDC", "USD")


def parse_symbol(s: str) -> Tuple[str, str]:
    """
    Return (base, quote) from any of:
      * BTCUSDT
      * BTC-USDT
      * BTC/USDT

    Fallback quote is USD if we can't recognize it.
    """
    s = (s or "").upper().strip()

    # 1) Hyphen / slash forms first (what your Sheet tends to use, e.g. BTC/USDT)
    for sep in ("-", "/"):
        if sep in s:
            b, q = (s.split(sep, 1) + [""])[:2]
            b = b or ""
            q = q or "USD"
            return b, q

    # 2) Plain concat form with common stables
    m = re.match(r"^([A-Z0-9]+?)(USDT|USDC|USD)$", s)
    if m:
        return m.group(1), m.group(2)

    # 3) Fallback: treat whole thing as "base", assume USD quote
    return s, "USD"


# ---------------------------------------------------------------------------
# Local BinanceUS client (no external dependency)
# ---------------------------------------------------------------------------


class BinanceUS:
    def __init__(self) -> None:
        self.api_key = os.getenv("BINANCEUS_API_KEY") or os.getenv("BINANCE_API_KEY")
        self.secret_key = (os.getenv("BINANCEUS_API_SECRET") or os.getenv("BINANCE_SECRET_KEY") or os.getenv("BINANCE_API_SECRET"))
        self.base_url = (os.getenv("BINANCEUS_BASE_URL") or os.getenv("BINANCE_BASE_URL") or "https://api.binance.us").rstrip("/")
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": "NovaTrade-Edge/3.0",
            }
        )

    # --- signing / request core -------------------------------------------

    def _sign(self, params: Dict[str, Any]) -> Dict[str, Any]:
        if not self.api_key or not self.secret_key:
            return params

        if "timestamp" not in params:
            params["timestamp"] = int(time.time() * 1000)

        query_string = "&".join(f"{k}={v}" for k, v in params.items())
        signature = hmac.new(
            self.secret_key.encode("utf-8"),
            query_string.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        params["signature"] = signature
        return params

    def _request(
        self, method: str, path: str, params: Optional[Dict[str, Any]] = None, signed: bool = False
    ) -> Any:
        url = f"{self.base_url}{path}"
        params = params or {}
        headers: Dict[str, str] = {}

        if signed:
            if not self.api_key:
                raise ValueError("Binance API credentials missing")
            headers["X-MBX-APIKEY"] = self.api_key
            params = self._sign(params)

        try:
            if method.upper() in ("GET", "DELETE"):
                resp = self.session.request(
                    method, url, params=params, headers=headers, timeout=10
                )
            else:
                resp = self.session.request(
                    method, url, data=params, headers=headers, timeout=10
                )

            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            _log(f"BinanceUS request error: {e}")
            if hasattr(e, "response") and getattr(e, "response") is not None:
                _log(f"Response: {e.response.text[:200]}")  # type: ignore[attr-defined]
            raise

    # --- public API wrappers ----------------------------------------------

    def account(self) -> Any:
        return self._request("GET", "/api/v3/account", signed=True)

    def get_order(
        self,
        symbol: str,
        clientOrderId: Optional[str] = None,
        origClientOrderId: Optional[str] = None,
    ) -> Any:
        params: Dict[str, Any] = {"symbol": symbol}
        if clientOrderId:
            params["origClientOrderId"] = clientOrderId
        if origClientOrderId:
            params["origClientOrderId"] = origClientOrderId
        return self._request("GET", "/api/v3/order", params=params, signed=True)

    def create_order(
        self,
        symbol: str,
        side: str,
        type: str,
        quantity: Optional[float] = None,
        quoteOrderQty: Optional[float] = None,
        newClientOrderId: Optional[str] = None,
    ) -> Any:
        params: Dict[str, Any] = {
            "symbol": symbol,
            "side": side,
            "type": type,
        }
        if quantity:
            params["quantity"] = quantity
        if quoteOrderQty:
            params["quoteOrderQty"] = quoteOrderQty
        if newClientOrderId:
            params["newClientOrderId"] = newClientOrderId

        return self._request("POST", "/api/v3/order", params=params, signed=True)


# ---------------------------------------------------------------------------
# Binance execution wrapper
# ---------------------------------------------------------------------------


def binance_execution_wrapper(
    venue_symbol: str,
    side: str,
    amount_quote: float = 0.0,
    amount_base: float = 0.0,
    client_id: str = "",
    edge_mode: str = "dry",
    edge_hold: bool = False,
) -> Dict[str, Any]:
    """
    Execute a market order on BinanceUS using our local client.
    """
    # HOLD path
    if str(edge_hold).lower() in ("true", "1", "yes"):
        return {
            "ok": False,
            "status": "held",
            "message": "EDGE_HOLD is enabled",
            "fills": [],
        }

    # Dry-run path
    if edge_mode == "dry":
        # Treat dry-run as "success" for pipeline validation, but with zeroed fills.
        return {
            "ok": True,
            "status": "filled",
            "filled": True,
            "fills": [],
            "message": "dry_run",
            # keep both keys for downstream compatibility
            "avg_price": 0.0,
            "average_price": 0.0,
            "executed_qty": float(amount_base or 0.0),
        }

    # Live execution
    try:
        api = BinanceUS()

        # Ensure Binance form, e.g. BTCUSDT
        symbol = venue_symbol.replace("-", "").replace("_", "").replace("/", "")

        kw: Dict[str, Any] = {"symbol": symbol, "side": side, "type": "MARKET"}
        if client_id:
            kw["newClientOrderId"] = client_id

        side_u = side.upper()

        if side_u == "BUY":
            if amount_quote > 0:
                kw["quoteOrderQty"] = amount_quote
            elif amount_base > 0:
                kw["quantity"] = amount_base
            else:
                raise ValueError("BUY requires amount_quote or amount_base")
        else:  # SELL
            if amount_base > 0:
                kw["quantity"] = amount_base
            elif amount_quote > 0:
                # Binance allows quoteOrderQty for MARKET orders both ways
                kw["quoteOrderQty"] = amount_quote
            else:
                raise ValueError("SELL requires amount_base or amount_quote")

        res = api.create_order(**kw)

        fills: List[Dict[str, float]] = []
        total_qty = 0.0
        total_cost = 0.0

        if "fills" in res:
            for f in res["fills"]:
                p = float(f.get("price", 0))
                q = float(f.get("qty", 0))
                fills.append({"price": p, "qty": q})
                total_qty += q
                total_cost += p * q

        avg_price = (total_cost / total_qty) if total_qty else 0.0

        filled = (res.get("status") in ("FILLED", "PARTIALLY_FILLED")) or (float(res.get("executedQty", 0) or 0.0) > 0.0)
        return {
            "ok": bool(filled),
            "status": "filled" if filled else "open",
            "filled": bool(filled),
            "fills": fills,
            "executed_qty": float(res.get("executedQty", 0)),
            # keep both keys for downstream compatibility
            "avg_price": avg_price,
            "average_price": avg_price,
            "raw": res,
        }

    except Exception as e:
        _log(f"Binance execution failed: {e}")
        return {"ok": False, "status": "error", "message": str(e), "fills": []}


# ---------------------------------------------------------------------------
# Venue executors
# ---------------------------------------------------------------------------

try:
    from executors.coinbase_advanced_executor import (
        execute_market_order as cb_exec,
        CoinbaseCDP,
    )
except Exception:
    cb_exec = None
    CoinbaseCDP = None  # type: ignore[misc]

try:
    from executors.kraken_executor import (
        execute_market_order as kr_exec,
        _balance as kraken_balance,
    )
except Exception:
    kr_exec = None

    def kraken_balance() -> Dict[str, float]:
        return {}


EXECUTORS = {
    "COINBASE": cb_exec,
    "COINBASEADV": cb_exec,
    "CBADV": cb_exec,
    "BINANCEUS": binance_execution_wrapper,
    "BUSA": binance_execution_wrapper,
    "KRAKEN": kr_exec,
}

# ---------------------------------------------------------------------------
# Policy gate (best-effort import)
# ---------------------------------------------------------------------------

try:
    from edge_pretrade import pretrade_validate
except Exception:
    def pretrade_validate(**kwargs):  # type: ignore[override]
        # Allow all if policy module missing
        return True, "ok", kwargs.get("quote"), 0.0, 0.0


# ---------------------------------------------------------------------------
# Telemetry / balances (optional)
# ---------------------------------------------------------------------------

try:
    import telemetry_db
except Exception:
    telemetry_db = None  # type: ignore[misc]

try:
    import telemetry_sync
except Exception:
    telemetry_sync = None  # type: ignore[misc]


def get_balances() -> Dict[str, Dict[str, float]]:
    out: Dict[str, Dict[str, float]] = {}

    # COINBASE
    try:
        if CoinbaseCDP:
            out["COINBASE"] = CoinbaseCDP().balances()
    except Exception as e:
        _log(f"balances COINBASE error: {e}")

    # BINANCEUS
    try:
        acct = BinanceUS().account()
        out["BINANCEUS"] = {
            (b.get("asset") or "").upper(): float(b.get("free") or 0.0)
            for b in (acct.get("balances") or [])
        }
    except Exception as e:
        _log(f"balances BINANCEUS error: {e}")

    # KRAKEN
    try:
        out["KRAKEN"] = kraken_balance() or {}
    except Exception as e:
        _log(f"balances KRAKEN error: {e}")

    return out


_last_hb = 0.0
_last_bal_snap = 0.0
_last_push_bal = 0.0


def maybe_heartbeat() -> None:
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
        except Exception as e:
            _log(f"telemetry_db heartbeat error: {e}")

    if telemetry_sync and hasattr(telemetry_sync, "send_heartbeat"):
        try:
            telemetry_sync.send_heartbeat(latency_ms=0)
        except Exception as e:
            _log(f"heartbeat push error: {e}")


def _printable_balances(bals: Dict[str, float], wanted: Tuple[str, ...]) -> Dict[str, float]:
    out: Dict[str, float] = {}
    for k in wanted:
        if k in bals:
            try:
                out[k] = round(float(bals[k]), 8)
            except Exception:
                out[k] = float(bals[k])  # type: ignore[arg-type]
    return out


def maybe_balance_snapshot() -> None:
    global _last_bal_snap
    now = time.time()
    if now - _last_bal_snap < BALANCE_SNAPSHOT_SECS:
        return
    _last_bal_snap = now

    bals = get_balances()
    if not bals:
        return

    if telemetry_db:
        try:
            for venue, v in bals.items():
                telemetry_db.upsert_balances(venue, v)
        except Exception as e:
            _log(f"telemetry_db balances error: {e}")

    view = {
        k: _printable_balances(v, ("USDT", "USDC", "USD", "BTC", "XBT"))
        for k, v in bals.items()
    }
    _log(f"snapshot balances: {view}")


def maybe_push_balances() -> None:
    global _last_push_bal
    if not PUSH_BALANCES_ENABLED or telemetry_sync is None:
        return
    now = time.time()
    if now - _last_push_bal < max(60, PUSH_BALANCES_EVERY_S):
        return
    _last_push_bal = now
    try:
        if hasattr(telemetry_sync, "push_telemetry"):
            telemetry_sync.push_telemetry()
            _log("balances push: ok")
    except Exception as e:
        _log(f"balances push error: {e}")


# ---------------------------------------------------------------------------
# Price fetch (public)
# ---------------------------------------------------------------------------


def fetch_price(venue: str, base: str, quote: str) -> float:
    v = re.sub(r"[^A-Z]", "", (venue or "").upper())
    try:
        if v in ("BINANCEUS", "BUSA"):
            sym = f"{base.upper()}{quote.upper()}"
            j = SESSION.get(
                "https://api.binance.us/api/v3/ticker/price",
                params={"symbol": sym},
                timeout=6,
            ).json()
            return float(j["price"])

        if v in ("COINBASE", "COINBASEADV", "CBADV"):
            prod = f"{base.upper()}-{quote.upper()}"
            j = SESSION.get(
                f"https://api.exchange.coinbase.com/products/{prod}/ticker", timeout=6
            ).json()
            return float(j["price"])

        if v == "KRAKEN":
            def _kr(a: str) -> str:
                return "XBT" if a.upper() == "BTC" else a.upper()

            pair = f"{_kr(base)}{_kr(quote)}"
            j = SESSION.get(
                "https://api.kraken.com/0/public/Ticker",
                params={"pair": pair},
                timeout=6,
            ).json()
            if j.get("error"):
                raise RuntimeError(",".join(j["error"]))
            res = next(iter(j["result"].values()))
            return float(res["c"][0])

    except Exception as e:
        _log(f"price fetch failed {venue} {base}/{quote}: {e}")

    return float("nan")


# ---------------------------------------------------------------------------
# Execution core
# ---------------------------------------------------------------------------


def _venue_key(v: str) -> str:
    return re.sub(r"[^A-Z]", "", (v or "").upper())


def resolve_symbol(venue_key: str, base: str, quote: str) -> str:
    v = _venue_key(venue_key)
    if v in ("COINBASE", "COINBASEADV", "CBADV"):
        return f"{base.upper()}-{quote.upper()}"
    if v in ("BINANCEUS", "BUSA"):
        return f"{base.upper()}{quote.upper()}"
    if v == "KRAKEN":
        def _kr(a: str) -> str:
            return "XBT" if a.upper() == "BTC" else a.upper()

        return f"{_kr(base)}{_kr(quote)}"
    return f"{base.upper()}-{quote.upper()}"


def normalize_amounts_from_intent(intent: Dict[str, Any], price: float) -> Dict[str, float]:
    """
    Robust sizing:
      - If amount_quote/quote_amount/amount_usd exists -> treat as quote spend
      - Else if flags contains 'quote' -> treat intent['amount'] as quote spend
      - Else treat intent['amount'] as base qty
    """
    out = {"amount_base": 0.0, "amount_quote": 0.0}

    # Explicit quote fields win
    amt_quote = (
        intent.get("amount_quote")
        or intent.get("quote_amount")
        or intent.get("amount_usd")
    )
    if amt_quote is not None:
        out["amount_quote"] = max(0.0, float(amt_quote or 0.0))
        if price and price > 0:
            out["amount_base"] = out["amount_quote"] / float(price)
        return out

    # Flag-driven quote mode (legacy)
    amt = float(intent.get("amount", 0) or 0)
    flags = {str(x).lower() for x in intent.get("flags", [])}
    if "quote" in flags:
        out["amount_quote"] = max(0.0, amt)
        if price and price > 0:
            out["amount_base"] = out["amount_quote"] / float(price)
        return out

    # Default: amount is base qty
    out["amount_base"] = max(0.0, amt)
    if price and price > 0:
        out["amount_quote"] = out["amount_base"] * float(price)
    return out

def execute_market(
    venue_key: str,
    base: str,
    quote: str,
    side: str,
    amount_quote: float = 0.0,
    amount_base: float = 0.0,
    client_id: str = "",
) -> Dict[str, Any]:
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


def exec_command(cmd: Dict[str, Any], balances_cache: Optional[Dict[str, Dict[str, float]]] = None) -> Dict[str, Any]:
    payload = cmd.get("intent") or cmd.get("payload") or {}
    venue = (payload.get("venue") or "").upper()
    side = (payload.get("side") or "").upper()
    symbol = payload.get("symbol") or payload.get("product_id") or ""

    base, quote = parse_symbol(symbol)
    px = fetch_price(venue, base, quote)
    sized = normalize_amounts_from_intent(payload, px)

    if EDGE_HOLD:
        return {
            "ok": False,
            "status": "held",
            "message": "EDGE_HOLD enabled",
            "fills": [],
            "venue": venue,
            "symbol": symbol,
            "side": side or "?",
            "price_usd": px if px == px else None,
        }

    # Infer side if missing
    if not side:
        if sized["amount_quote"] > 0:
            side = "BUY"
        elif sized["amount_base"] > 0:
            side = "SELL"
        else:
            return {
                "status": "error",
                "message": "missing side/amount",
                "fills": [],
                "venue": venue,
                "symbol": symbol,
                "side": side,
            }

    venue_balances = (balances_cache or {}).get(venue) or {}
    ok, reason, chosen_quote, *_ = pretrade_validate(
        venue=venue,
        base=base,
        quote=quote,
        price=px,
        amount_base=sized["amount_base"],
        amount_quote=sized["amount_quote"],
        venue_balances=venue_balances,
    )

    if not ok:
        return {
            "status": "error",
            "message": reason,
            "fills": [],
            "venue": venue,
            "symbol": symbol,
            "side": side,
        }

    if chosen_quote and chosen_quote != quote:
        quote = chosen_quote

    try:
        res = execute_market(
            venue_key=venue,
            base=base,
            quote=quote,
            side=side,
            amount_quote=sized["amount_quote"] if side == "BUY" else 0.0,
            amount_base=sized["amount_base"] if side == "SELL" else 0.0,
            client_id=str(cmd.get("id")),
        )
    except Exception as e:
        return {
            "status": "error",
            "message": str(e),
            "fills": [],
            "venue": venue,
            "symbol": symbol,
            "side": side,
        }

    res.setdefault("venue", venue)
    res.setdefault("symbol", f"{base}-{quote}")
    res.setdefault("side", side)
    if px == px:
        res.setdefault("price_usd", px)
    return res


# ---------------------------------------------------------------------------
# Pull / Ack
# ---------------------------------------------------------------------------

_receipts_path = pathlib.Path("receipts.jsonl")
RECENT_IDS: "collections.deque[str]" = collections.deque(maxlen=256)


def durable_ack(cmd_id: int, ok: bool, receipt: Dict[str, Any]) -> bool:
    """
    Append to local receipts.jsonl, then POST ack to Bus with retries.
    """
    try:
        with _receipts_path.open("a", encoding="utf-8") as f:
            f.write(
                json.dumps(
                    {
                        "ts": int(time.time()),
                        "agent_id": AGENT_ID,
                        "cmd_id": cmd_id,
                        "ok": bool(ok),
                        "receipt": receipt,
                    }
                )
                + "\n"
            )
    except Exception as e:
        _log(f"receipts.jsonl append error: {e}")

    body = {"agent_id": AGENT_ID, "cmd_id": cmd_id, "ok": bool(ok), "receipt": receipt}

    backoff = 1
    for _ in range(4):
        try:
            r = bus_post("/api/commands/ack", body, timeout=20)
            _log(f"ack {cmd_id} {r.status_code} {r.text[:160]}")
            if r.ok:
                return True
        except Exception as e:
            _log(f"ack error {cmd_id}: {e}")
        time.sleep(backoff)
        backoff = min(backoff * 2, 30)
    return False


def pull_once() -> List[Dict[str, Any]]:
    body = {"agent_id": AGENT_ID, "limit": MAX_PULL, "lease_seconds": LEASE_SECONDS}
    r = bus_post("/api/commands/pull", body, timeout=20)
    if r.status_code >= 400:
        raise requests.HTTPError(f"{r.status_code} {r.text[:200]}", response=r)
    try:
        j = r.json()
        return j.get("commands", []) if isinstance(j, dict) else []
    except Exception:
        return []


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------


def wait_for_bus(max_wait_s: int = 120) -> None:
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


def main() -> None:
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
                    res = {"status": "error", "message": str(e), "fills": []}

                status_str = str(res.get("status", "")).lower()
                ok_flag = res.get("ok")
                if isinstance(ok_flag, bool):
                    ok = ok_flag
                else:
                    # treat anything that is not an obvious error / noop as ok
                    ok = status_str not in ("error", "noop")

                receipt = {
                    "normalized": {
                        "receipt_id": f"edge-{AGENT_ID}-{int(time.time())}",
                        "venue": res.get("venue")
                        or (cmd.get("intent") or {}).get("venue"),
                        "symbol": res.get("symbol")
                        or (cmd.get("intent") or {}).get("symbol"),
                        "side": res.get("side")
                        or (cmd.get("intent") or {}).get("side"),
                        "executed_qty": res.get("executed_qty"),
                        "avg_price": res.get("avg_price"),
                        "fee": res.get("fee"),
                        "fee_asset": res.get("fee_asset"),
                        "status": res.get("status"),
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
            txt = getattr(getattr(he, "response", None), "text", str(he))
            _log(f"poll HTTP {code}: {txt[:200]}")
            backoff = min(backoff * 2, 30)
        except Exception as e:
            _log(f"poll error: {e}")
            backoff = min(backoff * 2, 30)

        time.sleep(EDGE_POLL_SECS if backoff <= 2 else backoff)


if __name__ == "__main__":
    main()


# ---------------------------------------------------------------------------
# EdgeAgent entrypoint (dict intent → executor)
# ---------------------------------------------------------------------------


def execute_market_order(intent: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Bridge dict-shaped intents from some external caller into this executor.
    """
    if not intent:
        return {"status": "noop", "message": "no intent provided"}

    venue = (intent.get("venue") or "BINANCEUS").upper()
    symbol = intent.get("symbol") or intent.get("pair") or "BTCUSDT"
    side = (intent.get("side") or "BUY").upper()

    amt = (
        intent.get("amount_quote")
        or intent.get("amount_usd")
        or intent.get("amount")
        or 0.0
    )

    try:
        amount_quote = float(amt)
    except Exception:
        amount_quote = 0.0

    payload = {
        "venue": venue,
        "symbol": symbol,
        "side": side,
        "amount": amount_quote,
        "flags": ["quote"],  # treat amount as quote currency
    }
    cmd = {
        "id": intent.get("id"),
        "intent": payload,
    }
    return exec_command(cmd)
