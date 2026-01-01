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
import hashlib
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

    # File-based creds
    path = (os.getenv("COINBASE_CDP_KEY_PATH") or "").strip()
    if path:
        try:
            with open(path, "r", encoding="utf-8") as f:
                j = json.load(f)
            # Common key shapes in CDP exports
            key_name = (j.get("name") or j.get("key_name") or j.get("api_key_name") or "").strip()
            private_key = (j.get("privateKey") or j.get("private_key") or j.get("api_private_key") or "").strip()
            if key_name and private_key:
                return key_name, private_key
        except Exception as e:
            _log(f"CDP key load failed from {path}: {e}")
            return "", ""

    # Env-based
    return key_name, private_key


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
        url = f"{self.base}{path}"
        headers = {
            "User-Agent": UA,
            "Authorization": f"Bearer {self._bearer_for(method, path)}",
            "Content-Type": "application/json",
        }
        data = json.dumps(body) if body is not None else None

        # A tiny retry helps with transient Coinbase 5xx / network blips.
        last = None
        for i in range(3):
            try:
                r = requests.request(method, url, headers=headers, data=data, timeout=TIMEOUT_S)
                return r
            except Exception as e:
                last = e
                time.sleep(1.25 * (i + 1))
        raise RuntimeError(f"COINBASE request failed: {last}")

    def balances(self) -> Dict[str, float]:
        """
        Return {asset_symbol: available_float} using brokerage accounts list.
        Hardened against Coinbase response shape variations.
        """
        r = self._req("GET", ACCTS_PATH, None)
        if not r.ok:
            raise RuntimeError(f"COINBASE accounts HTTP {r.status_code}: {r.text[:240]}")

        try:
            j = r.json()
        except Exception:
            raise RuntimeError(f"COINBASE accounts non-json: {r.text[:240]}")

        # Expected: {"accounts":[...]}
        accts = j.get("accounts") if isinstance(j, dict) else None
        if not isinstance(accts, list):
            # Some variations: {"data":{"accounts":[...]}}
            data = j.get("data") if isinstance(j, dict) else None
            if isinstance(data, dict) and isinstance(data.get("accounts"), list):
                accts = data.get("accounts")

        if not isinstance(accts, list):
            raise RuntimeError(f"COINBASE accounts response shape unexpected: keys={list(j.keys()) if isinstance(j, dict) else type(j)}")

        out: Dict[str, float] = {}

        for a in accts:
            if not isinstance(a, dict):
                continue

            # --- currency symbol ---
            cur = a.get("currency") or {}
            if isinstance(cur, dict):
                sym = cur.get("symbol") or cur.get("code") or ""
            else:
                sym = cur

            sym = (sym or "").upper().strip()
            if not sym:
                continue

            # --- available balance ---
            bal = a.get("available_balance")
            if isinstance(bal, dict):
                val = bal.get("value", 0)
            else:
                val = bal

            try:
                amt = float(val)
            except Exception:
                amt = 0.0

            out[sym] = amt

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
    intent: Optional[Dict[str, Any]] = None,
    *,
    venue_symbol: str = "",
    side: str = "BUY",
    amount_quote: float = 0.0,
    amount_base: float = 0.0,
    client_id: str = "",
    edge_mode: str = "dryrun",
    edge_hold: bool = False,
    **kwargs,
) -> Dict[str, Any]:
    """Execute a market order on Coinbase Advanced (CDP JWT).

    COMPATIBILITY:
      - Edge Agent calls: execute_market_order(intent_dict)
      - Legacy callers may call: execute_market_order(venue_symbol=..., side=..., amount_quote=...)

    INTENT FIELDS (best-effort):
      - symbol / pair : "BTC/USDC" or "BTC-USDC"
      - side          : BUY/SELL
      - amount_usd or amount_quote or amount : treated as QUOTE sizing
      - amount_base   : optional base sizing
      - mode / edge_mode, edge_hold
      - id/cmd_id used to synthesize a stable client_order_id if not provided
    """
    # Merge intent dict + kwargs into local variables (intent wins for core fields).
    it = intent if isinstance(intent, dict) else {}
    # Venue symbol (Coinbase product_id) normalization happens below.
    venue_symbol = (it.get("symbol") or it.get("pair") or venue_symbol or "BTC/USDC")
    requested = str(venue_symbol).upper()

    # Side
    side_uc = (it.get("side") or side or "BUY").upper()

    # Edge mode / hold
    edge_mode = str(it.get("mode") or it.get("edge_mode") or edge_mode or "dryrun").lower()
    edge_hold = bool(it.get("edge_hold") if "edge_hold" in it else edge_hold)

    # Amounts
    def _f(x: Any) -> float:
        try:
            return float(x)
        except Exception:
            return 0.0

    amount_quote = _f(it.get("amount_quote") or it.get("amount_usd") or it.get("amount") or amount_quote)
    amount_base = _f(it.get("amount_base") or amount_base)

    # Normalize symbol to Coinbase product_id (BTC/USDC -> BTC-USDC)
    symbol = _norm_symbol(str(venue_symbol))

    # Stable client order id (Coinbase accepts empty, but we prefer deterministic).
    client_id = (it.get("client_id") or it.get("client_order_id") or client_id or "").strip()
    if not client_id:
        cmd_id = it.get("id") or it.get("cmd_id") or int(time.time())
        agent = (it.get("agent_id") or it.get("agent") or os.getenv("EDGE_AGENT_ID") or os.getenv("AGENT_ID") or "edge")
        # Keep short: 36-ish chars max
        raw = f"{agent}:{cmd_id}:{symbol}:{side_uc}"
        digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:18]
        client_id = f"nt-{str(cmd_id)[:10]}-{digest}"

    # Safety: sizing must be > 0, but do not hard-fail on base sizing if quote is present.
    if amount_quote <= 0 and amount_base <= 0:
        return {
            "ok": False,
            "status": "error",
            "message": "amount_quote/amount_usd (quote sizing) or amount_base must be > 0",
            "fills": [],
            "venue": "COINBASE",
            "symbol": symbol,
            "requested_symbol": requested,
            "resolved_symbol": symbol,
            "side": side_uc,
        }

    # Optional precheck: Coinbase often rejects too-small orders. Let operator set a floor.
    try:
        min_quote = float(os.getenv("COINBASE_MIN_QUOTE", os.getenv("COINBASE_MIN_NOTIONAL", "0")) or 0.0)
    except Exception:
        min_quote = 0.0
    if min_quote and amount_quote and amount_quote < min_quote:
        return {
            "ok": False,
            "status": "rejected",
            "message": f"amount_quote below COINBASE_MIN_QUOTE floor ({amount_quote} < {min_quote})",
            "fills": [],
            "venue": "COINBASE",
            "symbol": symbol,
            "requested_symbol": requested,
            "resolved_symbol": symbol,
            "side": side_uc,
            "amount_quote": amount_quote,
            "amount_base": amount_base,
        }

    if edge_hold:
        return {
            "ok": True,
            "status": "held",
            "message": "EDGE_HOLD enabled",
            "fills": [],
            "venue": "COINBASE",
            "symbol": symbol,
            "requested_symbol": requested,
            "resolved_symbol": symbol,
            "side": side_uc,
            "amount_quote": amount_quote,
            "amount_base": amount_base,
            "client_id": client_id,
        }

    if edge_mode != "live":
        return {
            "ok": True,
            "status": "dryrun",
            "message": "dryrun mode — not placing live Coinbase order",
            "fills": [],
            "venue": "COINBASE",
            "symbol": symbol,
            "requested_symbol": requested,
            "resolved_symbol": symbol,
            "side": side_uc,
            "amount_quote": amount_quote,
            "amount_base": amount_base,
            "client_id": client_id,
        }

    # Live execution
    try:
        cb = CoinbaseCDP()

        # Snapshot balances pre (best effort)
        pre = {}
        try:
            pre = cb.balances()
        except Exception:
            pre = {}

        res = cb.place_market(
            product_id=symbol,
            side=side_uc,
            quote_size=float(amount_quote or 0.0),
            base_size=float(amount_base or 0.0),
            client_order_id=client_id,
        )

        # Snapshot balances post (best effort)
        post = {}
        try:
            post = cb.balances()
        except Exception:
            post = {}

        # Extract a txid/order_id if present
        txid = ""
        if isinstance(res, dict):
            txid = (
                res.get("order_id")
                or (res.get("success_response") or {}).get("order_id")
                or (res.get("order") or {}).get("order_id")
                or ""
            )

        return {
            "ok": True,
            "status": "ok",
            "venue": "COINBASE",
            "symbol": symbol,
            "requested_symbol": requested,
            "resolved_symbol": symbol,
            "side": side_uc,
            "txid": txid,
            "client_id": client_id,
            "fills": [],  # Coinbase may require separate fills endpoint; keep raw for Bus
            "raw": res,
            "pre_balances": pre,
            "post_balances": post,
        }

    except Exception as e:
        return {
            "ok": False,
            "status": "error",
            "message": str(e),
            "fills": [],
            "venue": "COINBASE",
            "symbol": symbol,
            "requested_symbol": requested,
            "resolved_symbol": symbol,
            "side": side_uc,
            "client_id": client_id,
        }
