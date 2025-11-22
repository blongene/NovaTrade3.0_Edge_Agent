#!/usr/bin/env python3
# executors/kraken_executor.py — MARKET executor with BUY quote-guard, SELL clamp, provenance, and balance snapshots.
import os, time, hmac, hashlib, base64, urllib.parse, requests
from typing import Dict, Any
from .kraken_util import to_kraken_altname

BASE = os.getenv("KRAKEN_BASE_URL", "https://api.kraken.com").rstrip("/")
KEY  = os.getenv("KRAKEN_KEY", "")
SEC  = os.getenv("KRAKEN_SECRET", "")  # base64 Kraken secret
TIMEOUT = int(os.getenv("KRAKEN_TIMEOUT_S", "15"))


def _sym(venue_symbol: str) -> str:
    """
    Map a generic symbol like 'OCEAN/USDT' into Kraken's pair altname.

    We use kraken_util.to_kraken_altname, with a simple BTC->XBT fallback if needed.
    """
    s = (venue_symbol or "BTC/USDT").upper()
    try:
        return to_kraken_altname(s)
    except Exception:
        s = s.replace("/", "")
        if s.startswith("BTC"):
            s = "XBT" + s[3:]
        return s


def _public(path, params=None):
    r = requests.get(f"{BASE}{path}", params=params or {}, timeout=TIMEOUT)
    r.raise_for_status()
    return r.json()


def _pair_info(pair: str) -> dict:
    try:
        j = _public("/0/public/AssetPairs", {"pair": pair})
        return next(iter(j["result"].values()))
    except Exception:
        return {
            "ordermin": "0.00005" if pair.startswith("XBT") else "0.0",
        }


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
    post = urllib.parse.urlencode(data)
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
    """Return balances keyed by Kraken assets (XBT, USDT, USDC...)."""
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


# ---------------------------------------------------------------------------
# Core executor
# ---------------------------------------------------------------------------
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
    """
    Core Kraken market executor.

    All returns are in normalized Edge format:

        {
          "normalized": True,
          "ok": True/False,
          "status": "ok" | "error" | ...,
          "venue": "KRAKEN",
          "symbol": "<kraken_pair>",
          ...
        }
    """
    requested = (venue_symbol or "BTC/USDT").upper()
    pair = _sym(requested)  # resolved to Kraken pair e.g., XBTUSDT
    side_uc = (side or "").upper()

    base_payload: Dict[str, Any] = {
        "normalized": True,
        "venue": "KRAKEN",
        "symbol": pair,
        "requested_symbol": requested,
