#!/usr/bin/env python3
# executors/kraken_executor.py — MARKET executor with BUY quote-guard, SELL clamp, provenance, and balance snapshots.
#
# DROP-IN HARDENING (Jan 2026):
# - Fix indentation bug in execute_market_order() that prevented live/dryrun routing for quote-sized intents.
# - Normalize receipt fields consistently (ok/status/message/fills/normalized).
# - Dryrun returns status="filled" (safe simulation) to match Bus/ledger expectations.
# - EDGE_HOLD returns ok:true status:"held" (non-error).
# - Quote/base balance guards derive assets from requested symbol (USD/USDT/USDC) with Kraken key aliases (ZUSD).
# - Live AddOrder: only include userref if numeric (Kraken expects int).
#
# Compatibility: Edge Agent calls execute_market_order(intent_dict)

import os, time, hmac, hashlib, base64, urllib.parse, requests
from typing import Dict, Any, Tuple, Optional
from .kraken_util import to_kraken_altname

# ---- Intent canonicalization helper (Bus/Edge compatibility) ----
try:
    from .common import canonicalize_order_place_intent
except Exception:  # pragma: no cover
    try:
        from executors.common import canonicalize_order_place_intent
    except Exception:
        from common import canonicalize_order_place_intent  # type: ignore


BASE = os.getenv("KRAKEN_BASE_URL", "https://api.kraken.com").rstrip("/")
KEY  = os.getenv("KRAKEN_KEY", "")
SEC  = os.getenv("KRAKEN_SECRET", "")  # base64 Kraken secret
TIMEOUT = int(os.getenv("KRAKEN_TIMEOUT_S", "15"))

# --- helpers ----------------------------------------------------------------
def _norm_receipt(base: Dict[str, Any], *, ok: bool, status: str, message: str, fills=None, **extra) -> Dict[str, Any]:
    out = {
        **base,
        "normalized": True,
        "ok": bool(ok),
        "status": status,
        "message": message,
        "fills": fills or [],
    }
    out.update(extra)
    return out

def _split_symbol(requested: str) -> Tuple[str, str]:
    s = (requested or "BTC/USDT").upper().strip()
    if "/" in s:
        b, q = s.split("/", 1)
        return b.strip(), q.strip()
    for q in ("USDT", "USDC", "USD"):
        if s.endswith(q) and len(s) > len(q):
            return s[:-len(q)], q
    return s, "USD"

def _balance_key_candidates(asset: str) -> Tuple[str, ...]:
    a = (asset or "").upper().strip()
    if a == "USD":
        return ("ZUSD", "USD")
    if a == "BTC":
        return ("XBT", "BTC")
    return (a,)

def _sym(venue_symbol: str) -> str:
    s = (venue_symbol or "BTC/USDT").upper()
    try:
        return to_kraken_altname(s)
    except Exception:
        s2 = s.replace("/", "")
        if s2.startswith("BTC"):
            s2 = "XBT" + s2[3:]
        return s2

def _public(path, params=None):
    r = requests.get(f"{BASE}{path}", params=params or {}, timeout=TIMEOUT)
    r.raise_for_status()
    return r.json()

def _pair_info(pair: str) -> dict:
    try:
        j = _public("/0/public/AssetPairs", {"pair": pair})
        return next(iter(j["result"].values()))
    except Exception:
        return {"ordermin": "0.00005" if pair.startswith("XBT") else "0.0"}

def _ticker_price(pair: str) -> float:
    try:
        j = _public("/0/public/Ticker", {"pair": pair})
        result = next(iter(j["result"].values()))
        return float(result["c"][0])
    except Exception:
        return 0.0

def _sign(path: str, data: dict) -> dict:
    nonce = str(int(time.time() * 1000))
    data = {**data, "nonce": nonce}
    post = urllib.parse.urlencode({k: v for k, v in data.items() if v is not None})
    sha256 = hashlib.sha256((nonce + post).encode()).digest()
    sig = base64.b64encode(
        hmac.new(base64.b64decode(SEC), path.encode() + sha256, hashlib.sha512).digest()
    ).decode()
    return {"hdr": {"API-Key": KEY, "API-Sign": sig}, "qs": post}

def _private(path: str, data: dict):
    s = _sign(path, data)
    r = requests.post(f"{BASE}{path}", data=s["qs"], headers=s["hdr"], timeout=TIMEOUT)
    j = r.json()
    if j.get("error"):
        raise RuntimeError(",".join(j["error"]))
    return j["result"]

def _balance() -> Dict[str, float]:
    s = _sign("/0/private/Balance", {})
    r = requests.post(f"{BASE}/0/private/Balance", data=s["qs"], headers=s["hdr"], timeout=TIMEOUT)
    j = r.json()
    if j.get("error"):
        raise RuntimeError(",".join(j["error"]))
    out: Dict[str, float] = {}
    for k, v in (j.get("result") or {}).items():
        try:
            out[k] = float(v)
        except Exception:
            continue
    return out

def _get_free(bals: Dict[str, float], asset: str) -> float:
    for k in _balance_key_candidates(asset):
        if k in bals:
            try:
                return float(bals.get(k) or 0.0)
            except Exception:
                return 0.0
    return 0.0

def _execute_market_order_core(
    *,
    venue_symbol: str,
    side: str,
    amount_quote: float = 0.0,
    amount_base: float = 0.0,
    client_id: str = "",
    edge_mode: str = "dryrun",
    edge_hold: bool = False,
    **_
) -> Dict[str, Any]:
    requested = (venue_symbol or "BTC/USDT").upper()
    base_asset, quote_asset = _split_symbol(requested)
    pair = _sym(requested)
    side_uc = (side or "").upper()

    base_payload: Dict[str, Any] = {
        "venue": "KRAKEN",
        "symbol": pair,
        "requested_symbol": requested,
        "resolved_symbol": pair,
        "side": side_uc,
        "client_id": client_id,
        "base_asset": base_asset,
        "quote_asset": quote_asset,
    }

    if edge_hold:
        return _norm_receipt(base_payload, ok=True, status="held", message="EDGE_HOLD enabled")

    if edge_mode != "live":
        px = _ticker_price(pair) or 60000.0
        qty = round((float(amount_quote or 0) / px) if side_uc == "BUY" else float(amount_base or 0), 8)
        return _norm_receipt(
            base_payload,
            ok=True,
            status="filled",
            message="kraken dryrun simulated fill (no live order placed)",
            fills=[{"qty": qty, "price": px}],
            executed_qty=qty,
            avg_price=px,
            dry_run=True,
            txid=f"SIM-KR-{int(time.time() * 1000)}",
        )

    if not (KEY and SEC):
        return _norm_receipt(base_payload, ok=False, status="error", message="Missing KRAKEN_KEY/KRAKEN_SECRET")

    info = _pair_info(pair)
    default_min = 0.00005 if pair.startswith("XBT") else 0.0
    try:
        ordermin = float(info.get("ordermin", default_min) or default_min)
    except Exception:
        ordermin = default_min

    try:
        bals = _balance()
    except Exception:
        bals = {}

    if side_uc == "BUY":
        q_spend = float(amount_quote or 0.0)
        if q_spend <= 0:
            return _norm_receipt(base_payload, ok=False, status="error", message="BUY requires amount_quote > 0")

        free_q = _get_free(bals, quote_asset)
        if q_spend > free_q:
            return _norm_receipt(
                base_payload,
                ok=False,
                status="error",
                message=f"insufficient {quote_asset}: have {free_q:.2f}, need {q_spend:.2f}",
                pre_balances={quote_asset: free_q},
            )

        px = _ticker_price(pair) or 0.0
        qty = round((q_spend / (px or 1.0)), 8)
        if qty < ordermin:
            return _norm_receipt(base_payload, ok=False, status="error", message=f"min volume {ordermin:.8f} not met")
    else:
        qty_req = float(amount_base or 0.0)
        free_b = _get_free(bals, base_asset)
        qty = round(min(max(0.0, qty_req), max(0.0, free_b - 1e-8)), 8)
        if qty < ordermin:
            return _norm_receipt(
                base_payload,
                ok=False,
                status="error",
                message=f"qty {qty:.8f} < ordermin {ordermin:.8f}",
                pre_balances={base_asset: free_b},
            )

    userref: Optional[int] = None
    try:
        if str(client_id).isdigit():
            userref = int(str(client_id))
    except Exception:
        userref = None

    try:
        res = _private(
            "/0/private/AddOrder",
            {
                "pair": pair,
                "type": "buy" if side_uc == "BUY" else "sell",
                "ordertype": "market",
                "volume": f"{qty:.8f}",
                "userref": userref,
            },
        )
    except RuntimeError as e:
        return _norm_receipt(base_payload, ok=False, status="error", message=str(e))

    txid = (res.get("txid") or [None])[0]

    post = {}
    try:
        b2 = _balance()
        post = {quote_asset: _get_free(b2, quote_asset), base_asset: _get_free(b2, base_asset)}
    except Exception:
        pass

    return _norm_receipt(
        base_payload,
        ok=True,
        status="filled" if txid else "open",
        message="kraken live order accepted" if txid else "kraken response parsed",
        txid=txid or f"KR-NOORD-{int(time.time() * 1000)}",
        post_balances=post,
    )

def execute_market_order(intent: dict | None = None) -> Dict[str, Any]:
    if not intent:
        return {"status": "noop", "message": "no intent provided", "normalized": True, "ok": False}

    venue_symbol = intent.get("symbol") or intent.get("pair") or "BTC/USDT"
    side = intent.get("side") or "BUY"

    def _f(x) -> float:
        try:
            return float(x)
        except Exception:
            return 0.0

    raw_amount = intent.get("amount")
    amount_quote = _f(intent.get("amount_quote") or intent.get("amount_usd") or 0.0)
    amount_base = _f(intent.get("amount_base") or 0.0)

    if amount_quote == 0.0 and raw_amount is not None and amount_base == 0.0:
        a = _f(raw_amount)
        if a > 0:
            amount_quote = a

    edge_mode = str(intent.get("mode") or os.getenv("EDGE_MODE", "dryrun")).lower()
    edge_hold_flag = bool(
        str(os.getenv("EDGE_HOLD", "false")).strip().lower() == "true"
        or intent.get("edge_hold") is True
    )

    client_id = str(intent.get("client_id") or intent.get("intent_id") or intent.get("id") or "")

    return _execute_market_order_core(
        venue_symbol=str(venue_symbol),
        side=str(side),
        amount_quote=float(amount_quote or 0.0),
        amount_base=float(amount_base or 0.0),
        client_id=client_id,
        edge_mode=edge_mode,
        edge_hold=edge_hold_flag,
    )
