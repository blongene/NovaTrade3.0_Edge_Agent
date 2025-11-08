# executors/coinbase_advanced_executor.py
# Coinbase Advanced Trade via CDP JWT. BUY uses quote_size (USDC); SELL uses base_size.
import os, time, json, requests
from typing import Dict, Any

CB_BASE       = os.getenv("COINBASE_BASE_URL", "https://api.coinbase.com").rstrip("/")
ORDERS_PATH   = "/api/v3/brokerage/orders"
ACCTS_PATH    = "/api/v3/brokerage/accounts"
TIMEOUT_S     = int(os.getenv("COINBASE_TIMEOUT_S", "20"))
CDP_KEY_PATH  = os.getenv("COINBASE_CDP_KEY_PATH", "cdp_api_key.json").strip()
UA            = "NovaTrade-Edge-CBAdv/2.2"

# Requires: pip install coinbase-advanced-py
try:
    from coinbase import jwt_generator
except Exception:
    jwt_generator = None

def _load_cdp_creds():
    key_name = None; private_key = None
    if CDP_KEY_PATH and os.path.exists(CDP_KEY_PATH):
        try:
            with open(CDP_KEY_PATH, "r", encoding="utf-8") as f:
                j = json.load(f)
                key_name = (j.get("name") or "").strip()
                private_key = (j.get("privateKey") or "").strip()
        except Exception:
            pass
    if not key_name or not private_key:
        key_name = (os.getenv("COINBASE_CDP_API_KEY_NAME") or "").strip()
        private_key = (os.getenv("COINBASE_CDP_PRIVATE_KEY") or "").strip()
    return (key_name if key_name else None, private_key if private_key else None)

class CoinbaseCDP:
    def __init__(self):
        self.base = CB_BASE
        self.sess = requests.Session()
        self.sess.headers.update({"User-Agent": UA})
        self.key_name, self.private_key = _load_cdp_creds()

    def _ensure_ready(self):
        if not jwt_generator:
            raise RuntimeError("coinbase-advanced-py is required (pip install coinbase-advanced-py)")
        if not (self.key_name and self.private_key):
            raise RuntimeError("Missing CDP creds (COINBASE_CDP_KEY_PATH or env pair)")

    def _bearer_for(self, method: str, path: str) -> str:
        self._ensure_ready()
        uri = jwt_generator.format_jwt_uri(method.upper(), path)
        return jwt_generator.build_rest_jwt(uri, self.key_name, self.private_key)

    def _call(self, method: str, path: str, *, body: Dict[str, Any] | None = None, tries=3):
        raw = json.dumps(body or {}, separators=(",", ":"), sort_keys=True)
        for i in range(tries):
            hdrs = {
                "Authorization": f"Bearer {self._bearer_for(method, path)}",
                "Accept": "application/json",
                "Content-Type": "application/json",
                "User-Agent": UA,
            }
            url = self.base + path
            r = self.sess.request(
                method.upper(), url,
                data=(raw if body is not None else None),
                headers=hdrs, timeout=TIMEOUT_S
            )
            if r.status_code < 500:
                return r
            time.sleep(1.25 * (i + 1))
        return r

    def place_market(self, *, product_id: str, side: str,
                     quote_size: float = 0.0, base_size: float = 0.0,
                     client_order_id: str = ""):
        side_uc = (side or "").upper()
        if side_uc not in {"BUY", "SELL"}:
            raise ValueError("invalid side")
        cfg = ({"market_market_ioc": {"quote_size": str(float(quote_size or 0.0))}}
               if side_uc == "BUY" else
               {"market_market_ioc": {"base_size": str(float(base_size or 0.0))}})
        body = {
            "client_order_id": str(client_order_id or f"NT-{int(time.time()*1000)}"),
            "product_id": product_id,
            "side": side_uc,
            "order_configuration": cfg,
        }
        return self._call("POST", ORDERS_PATH, body=body)

    def balances(self) -> Dict[str, float]:
        """Return {asset_symbol: available_float} using brokerage accounts list."""
        r = self._call("GET", ACCTS_PATH, body=None)
        j = r.json() if r.headers.get("content-type","").startswith("application/json") else {}
        out: Dict[str, float] = {}
        for a in (j.get("accounts") or []):
            sym = (a.get("currency") or a.get("asset_id") or "").upper()
            try:
                av = float((a.get("available_balance") or {}).get("value") or 0)
            except Exception:
                av = 0.0
            if sym:
                out[sym] = av
        return out

def _norm_symbol(venue_symbol: str) -> str:
    # BTC/USDC -> BTC-USDC (Coinbase product_id style)
    return (venue_symbol or "BTC/USDC").upper().replace("/", "-").strip()

def execute_market_order(*, venue_symbol: str, side: str,
                         amount_quote: float = 0.0, amount_base: float = 0.0,
                         client_id: str = "", edge_mode: str = "dryrun",
                         edge_hold: bool = False, **_):
    requested = (venue_symbol or "BTC/USDC").upper()
    symbol = _norm_symbol(venue_symbol)  # resolved product_id like BTC-USDC
    side_uc = (side or "").upper()

    # HOLD
    if edge_hold:
        return {"status":"held","message":"EDGE_HOLD enabled","fills":[],
                "venue":"COINBASE","symbol":symbol,
                "requested_symbol":requested,"resolved_symbol":symbol,"side":side_uc}

    # DRYRUN
    if edge_mode != "live":
        px = 60000.0
        qty = round(float(amount_base or (amount_quote or 0)/px), 8) if side_uc=="SELL" else round((amount_quote or 0)/px, 8)
        return {"status":"ok","txid":f"SIM-CB-{int(time.time()*1000)}","fills":[{"qty":qty,"price":px}],
                "venue":"COINBASE","symbol":symbol,
                "requested_symbol":requested,"resolved_symbol":symbol,
                "side":side_uc,"executed_qty":qty,"avg_price":px,
                "message":"coinbase dryrun simulated fill"}

    # LIVE
    try:
        cb = CoinbaseCDP()
        # Guards
        if side_uc == "BUY":
            spend = float(amount_quote or 0.0)
            if spend <= 0:
                return {"status":"error","message":"BUY requires amount_quote > 0","fills":[],
                        "venue":"COINBASE","symbol":symbol,
                        "requested_symbol":requested,"resolved_symbol":symbol,"side":side_uc}
            bals = cb.balances()
            free_usdc = float(bals.get("USDC", 0.0))
            if spend > free_usdc:
                return {"status":"error","message":f"insufficient USDC: have {free_usdc:.2f}, need {spend:.2f}",
                        "fills":[], "venue":"COINBASE","symbol":symbol,
                        "requested_symbol":requested,"resolved_symbol":symbol,"side":side_uc}
            resp = cb.place_market(product_id=symbol, side="BUY",
                                   quote_size=spend, client_order_id=str(client_id))
        else:
            bals = cb.balances()
            free_btc = float(bals.get("BTC", 0.0))
            qty_req = float(amount_base or 0.0)
            qty = min(max(0.0, qty_req), max(0.0, free_btc - 1e-8))
            qty = float(f"{qty:.8f}")  # Coinbase supports up to 8 dp for BTC
            if qty <= 0:
                return {"status":"error","message":"Insufficient free balance after clamp","fills":[],
                        "venue":"COINBASE","symbol":symbol,
                        "requested_symbol":requested,"resolved_symbol":symbol,"side":side_uc}
            resp = cb.place_market(product_id=symbol, side="SELL",
                                   base_size=qty, client_order_id=str(client_id))

        # Parse response
        try:
            data = resp.json()
        except Exception:
            data = {"raw": resp.text}
        if resp.status_code >= 400:
            return {"status":"error","message":f"{resp.status_code} {data}","fills":[],
                    "venue":"COINBASE","symbol":symbol,
                    "requested_symbol":requested,"resolved_symbol":symbol,"side":side_uc}

        order_id = (data.get("success_response") or {}).get("order_id") or data.get("order_id") or ""
        # Snapshot balances
        post = {}
        try:
            bb = cb.balances()
            post = {"USDC": float(bb.get("USDC", 0.0)), "BTC": float(bb.get("BTC", 0.0))}
        except Exception:
            pass

        return {"status":"ok","txid": order_id or f"CB-NOORD-{int(time.time()*1000)}",
                "fills": data.get("fills") or [],
                "venue":"COINBASE","symbol":symbol,
                "requested_symbol":requested,"resolved_symbol":symbol,
                "side":side_uc,"post_balances": post,
                "message":"coinbase live order accepted" if order_id else "coinbase response parsed"}
    except Exception as e:
        return {"status":"error","message":f"coinbase executor exception: {e}","fills":[],
                "venue":"COINBASE","symbol":symbol,
                "requested_symbol":requested,"resolved_symbol":symbol,"side":side_uc}
# --- universal drop-in for EdgeAgent ---
def execute_market_order(intent: dict = None):
    """EdgeAgent entrypoint shim."""
    from typing import Dict
    if intent is None:
        return {"status": "noop", "message": "no intent provided"}
    return execute(intent) if "execute" in globals() else {"status": "ok", "message": "simulated exec"}
