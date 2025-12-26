# executors/coinbase_advanced_executor.py
# Coinbase Advanced Trade (CDP JWT) executor + balance snapshot
#
# BULLETPROOF PATCH (Dec 2025):
# - Some environments expose jwt_generator at different import paths.
# - Your Edge runtime currently has `import coinbase` OK, but likely lacks `coinbase.jwt_generator`.
# - This module now discovers jwt_generator from multiple paths and prints ONE clean diagnostic line.

from __future__ import annotations

import os
import time
import json
import importlib
from typing import Dict, Any, Optional

import requests


CB_BASE = os.getenv("COINBASE_BASE_URL", "https://api.coinbase.com")  # Advanced Trade base
ACCTS_PATH = "/api/v3/brokerage/accounts"
ORDERS_PATH = "/api/v3/brokerage/orders"
TIMEOUT_S = int(os.getenv("COINBASE_TIMEOUT_S", "15"))
UA = os.getenv("EDGE_USER_AGENT", "NovaTradeEdge/3.0")


def _log(msg: str) -> None:
    # Keep Edge logs low-noise but visible.
    print(f"[coinbase_adv] {msg}")


def _load_cdp_creds() -> tuple[str, str]:
    """
    Supports:
      - COINBASE_CDP_KEY_PATH pointing at Render secret file (/etc/secrets/cdp_api_key.json)
      - or explicit env pair: COINBASE_CDP_KEY_NAME + COINBASE_CDP_PRIVATE_KEY
    """
    key_name = (os.getenv("COINBASE_CDP_KEY_NAME") or "").strip()
    private_key = (os.getenv("COINBASE_CDP_PRIVATE_KEY") or "").strip()

    if key_name and private_key:
        return key_name, private_key

    path = (os.getenv("COINBASE_CDP_KEY_PATH") or "").strip()
    if not path:
        # common default in Render
        path = "/etc/secrets/cdp_api_key.json"

    try:
        with open(path, "r", encoding="utf-8") as f:
            j = json.load(f)
        # CDP key JSON format (as you confirmed): {"name": "...", "privateKey": "..."}
        key_name = (j.get("name") or "").strip()
        private_key = (j.get("privateKey") or "").strip()
        return key_name, private_key
    except Exception as e:
        _log(f"CDP key load failed from {path}: {e}")
        return "", ""


def _discover_jwt_generator():
    """
    Try multiple known/likely module paths.
    We require the module to expose:
      - format_jwt_uri(method, path)
      - build_rest_jwt(uri, key_name, private_key)
    """
    candidates = [
        # common in coinbase-advanced-py installs:
        "coinbase.jwt_generator",
        "coinbase.jwt",
        # sometimes packaged differently:
        "coinbase_advanced_py.jwt_generator",
        "coinbase_advanced_py.jwt",
        "coinbase_advanced.jwt_generator",
        "coinbase_advanced.jwt",
        # last resort: attribute on `coinbase` package (some versions):
        "coinbase",
    ]

    for mod in candidates:
        try:
            m = importlib.import_module(mod)
        except Exception:
            continue

        # If they imported `coinbase`, jwt_generator may be an attribute.
        if mod == "coinbase" and hasattr(m, "jwt_generator"):
            m = getattr(m, "jwt_generator")

        if hasattr(m, "format_jwt_uri") and hasattr(m, "build_rest_jwt"):
            _log(f"jwt_generator OK via import '{mod}'")
            return m

    _log("jwt_generator NOT FOUND in known paths (coinbase-advanced-py install/import mismatch).")
    return None


jwt_generator = _discover_jwt_generator()


class CoinbaseCDP:
    def __init__(self):
        self.base = CB_BASE.rstrip("/")
        self.sess = requests.Session()
        self.sess.headers.update({"User-Agent": UA})
        self.key_name, self.private_key = _load_cdp_creds()

    def _ensure_ready(self):
        if not jwt_generator:
            # This message is intentionally explicit: it’s the single most common failure mode.
            raise RuntimeError(
                "Missing jwt_generator. Your environment has `import coinbase` but not a compatible "
                "jwt_generator module. Ensure coinbase-advanced-py is installed and not shadowed by "
                "a different `coinbase` package."
            )
        if not (self.key_name and self.private_key):
            raise RuntimeError("Missing CDP creds (COINBASE_CDP_KEY_PATH or env pair).")

    def _bearer_for(self, method: str, path: str) -> str:
        self._ensure_ready()
        uri = jwt_generator.format_jwt_uri(method.upper(), path)
        return jwt_generator.build_rest_jwt(uri, self.key_name, self.private_key)

    def _req(self, method: str, path: str, body: Optional[dict] = None) -> requests.Response:
        bearer = self._bearer_for(method, path)
        hdrs = {"Authorization": f"Bearer {bearer}", "Content-Type": "application/json"}

        raw = None
        if body is not None:
            raw = json.dumps(body, separators=(",", ":"))

        # basic retry on 5xx
        r: requests.Response = None  # type: ignore
        for i in range(3):
            url = self.base + path
            r = self.sess.request(
                method.upper(),
                url,
                data=(raw if body is not None else None),
                headers=hdrs,
                timeout=TIMEOUT_S,
            )
            if r.status_code < 500:
                return r
            time.sleep(1.25 * (i + 1))
        return r

    def balances(self) -> Dict[str, float]:
        """
        Return {asset_symbol: available_float} using brokerage accounts list.
        If Coinbase auth fails, we raise with a short message so upstream can log once.
        """
        r = self._req("GET", ACCTS_PATH, None)
        if not r.ok:
            # keep this tight: upstream logger will include it.
            raise RuntimeError(f"COINBASE accounts HTTP {r.status_code}: {r.text[:240]}")

        j = {}
        try:
            j = r.json()
        except Exception:
            raise RuntimeError("COINBASE accounts returned non-JSON response")

        out: Dict[str, float] = {}
        for a in (j.get("accounts") or []):
            sym = ((a.get("currency") or {}).get("code") or "").upper().strip()
            try:
                av = float((a.get("available_balance") or {}).get("value") or 0)
            except Exception:
                av = 0.0
            if sym:
                out[sym] = av
        return out

    def place_market(
        self,
        *,
        product_id: str,
        side: str,
        quote_size: float = 0.0,
        base_size: float = 0.0,
        client_order_id: str = "",
    ) -> Dict[str, Any]:
        side_uc = (side or "").upper()
        if side_uc not in {"BUY", "SELL"}:
            raise ValueError("side must be BUY or SELL")

        body: Dict[str, Any] = {
            "client_order_id": client_order_id or "",
            "product_id": product_id,
            "side": side_uc,
            "order_configuration": {
                "market_market_ioc": {}
            },
        }

        cfg = body["order_configuration"]["market_market_ioc"]
        if quote_size and quote_size > 0:
            cfg["quote_size"] = str(float(quote_size))
        elif base_size and base_size > 0:
            cfg["base_size"] = str(float(base_size))
        else:
            raise ValueError("Must provide quote_size or base_size")

        r = self._req("POST", ORDERS_PATH, body)
        if not r.ok:
            raise RuntimeError(f"COINBASE order HTTP {r.status_code}: {r.text[:240]}")

        try:
            return r.json()
        except Exception:
            return {"raw": r.text}


def _norm_symbol(venue_symbol: str) -> str:
    # BTC/USDC -> BTC-USDC (Coinbase product_id style)
    return (venue_symbol or "BTC/USDC").upper().replace("/", "-").strip()


def execute_market_order(
    *,
    venue_symbol: str,
    side: str,
    amount_quote: float = 0.0,
    amount_base: float = 0.0,
    client_id: str = "",
    edge_mode: str = "dryrun",
    edge_hold: bool = False,
    **_,
) -> Dict[str, Any]:
    requested = (venue_symbol or "BTC/USDC").upper()
    symbol = _norm_symbol(venue_symbol)
    side_uc = (side or "").upper()

    if edge_hold:
        return {
            "status": "held",
            "message": "EDGE_HOLD enabled",
            "fills": [],
            "venue": "COINBASE",
            "symbol": symbol,
            "requested_symbol": requested,
            "resolved_symbol": symbol,
            "side": side_uc,
        }

    if (edge_mode or "").lower() != "live":
        return {
            "status": "dryrun",
            "message": "dryrun mode — not placing live Coinbase order",
            "fills": [],
            "venue": "COINBASE",
            "symbol": symbol,
            "requested_symbol": requested,
            "resolved_symbol": symbol,
            "side": side_uc,
            "amount_quote": float(amount_quote or 0.0),
            "amount_base": float(amount_base or 0.0),
        }

    try:
        cb = CoinbaseCDP()

        # Snapshot balances pre
        pre = cb.balances()

        # Place market order
        res = cb.place_market(
            product_id=symbol,
            side=side_uc,
            quote_size=float(amount_quote or 0.0),
            base_size=float(amount_base or 0.0),
            client_order_id=(client_id or ""),
        )

        # Snapshot balances post (best effort)
        post = {}
        try:
            post = cb.balances()
        except Exception:
            post = {}

        return {
            "status": "ok",
            "venue": "COINBASE",
            "symbol": symbol,
            "requested_symbol": requested,
            "resolved_symbol": symbol,
            "side": side_uc,
            "raw": res,
            "pre_balances": pre,
            "post_balances": post,
        }

    except Exception as e:
        return {"status": "error", "message": str(e), "fills": [], "venue": "COINBASE", "symbol": symbol}
