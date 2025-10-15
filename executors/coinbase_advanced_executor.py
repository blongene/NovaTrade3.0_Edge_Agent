# executors/coinbase_advanced_executor.py
# Advanced Trade (CDP) auth via JWT using a CDP secret file (cdp_api_key.json)
import os, time, json, requests

CB_BASE       = os.getenv("COINBASE_BASE_URL", "https://api.coinbase.com").rstrip("/")
ORDERS_PATH   = "/api/v3/brokerage/orders"
TIMEOUT_S     = int(os.getenv("COINBASE_TIMEOUT_S", "20"))
CDP_KEY_PATH  = os.getenv("COINBASE_CDP_KEY_PATH", "cdp_api_key.json").strip()
UA            = "NovaTrade-Edge-CBAdv/2.0"

# We rely on Coinbase's official helper for correct JWT formatting/claims.
#   pip install coinbase-advanced-py
try:
    from coinbase import jwt_generator
except Exception as _e:
    jwt_generator = None

def _load_cdp_creds():
    """
    Reads the CDP secret from:
      1) COINBASE_CDP_KEY_PATH (JSON with fields: {"name": "...", "privateKey": "...PEM..."})
      2) or env fallbacks: COINBASE_CDP_API_KEY_NAME + COINBASE_CDP_PRIVATE_KEY
    Returns (key_name, private_key_pem) or (None, None) if missing.
    """
    key_name = None
    private_key = None

    # Preferred: secret file
    if CDP_KEY_PATH and os.path.exists(CDP_KEY_PATH):
        try:
            with open(CDP_KEY_PATH, "r", encoding="utf-8") as f:
                j = json.load(f)
            key_name   = (j.get("name") or "").strip()
            private_key = (j.get("privateKey") or "").strip()
        except Exception:
            key_name, private_key = None, None

    # Fallback: env vars
    if not key_name or not private_key:
        key_name = (os.getenv("COINBASE_CDP_API_KEY_NAME") or "").strip()
        private_key = (os.getenv("COINBASE_CDP_PRIVATE_KEY") or "").strip()

    return (key_name if key_name else None,
            private_key if private_key else None)

class CoinbaseCDP:
    """
    Minimal CDP client for Advanced Trade REST calls:
      - Generates a short-lived JWT per request with the official helper.
      - Sends Authorization: Bearer <jwt>
    """
    def __init__(self):
        self.base  = CB_BASE
        self.sess  = requests.Session()
        self.sess.headers.update({"User-Agent": UA})
        self.key_name, self.private_key = _load_cdp_creds()

    def _ensure_ready(self):
        if not jwt_generator:
            raise RuntimeError("coinbase-advanced-py is required (pip install coinbase-advanced-py)")
        if not (self.key_name and self.private_key):
            raise RuntimeError("Missing CDP creds: set COINBASE_CDP_KEY_PATH or COINBASE_CDP_API_KEY_NAME/COINBASE_CDP_PRIVATE_KEY")

    def _bearer_for(self, method: str, path: str) -> str:
        """
        Build a per-request JWT for the REST endpoint.
        Tokens expire quickly (~2 minutes); always create fresh per call.
        """
        self._ensure_ready()
        uri = jwt_generator.format_jwt_uri(method.upper(), path)
        return jwt_generator.build_rest_jwt(uri, self.key_name, self.private_key)

    def _post_retry(self, path: str, body: dict, tries=3):
        raw = json.dumps(body, separators=(",", ":"), sort_keys=True)
        for i in range(tries):
            token = self._bearer_for("POST", path)
            hdrs = {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
                "Accept": "application/json",
                "User-Agent": UA,
            }
            r = self.sess.post(self.base + path, data=raw, headers=hdrs, timeout=TIMEOUT_S)
            if r.status_code < 500:
                return r
            time.sleep(1.5 * (i + 1))
        return r

    def place_market(self, *, product_id: str, side: str,
                     quote_size: float = 0.0, base_size: float = 0.0,
                     client_order_id: str = ""):
        """
        For BUY: use quote_size (e.g., USDC amount).
        For SELL: use base_size  (e.g., BTC quantity).
        """
        side_uc = (side or "").upper()
        if side_uc not in {"BUY", "SELL"}:
            raise ValueError(f"invalid side: {side}")

        if side_uc == "BUY":
            cfg = {"market_market_ioc": {"quote_size": str(float(quote_size or 0.0))}}
        else:
            cfg = {"market_market_ioc": {"base_size":  str(float(base_size  or 0.0))}}

        body = {
            "client_order_id": str(client_order_id or f"NT-{int(time.time()*1000)}"),
            "product_id": product_id,   # e.g., BTC-USDC
            "side": side_uc,            # MUST be uppercase: BUY | SELL
            "order_configuration": cfg
        }
        return self._post_retry(ORDERS_PATH, body)

def execute_market_order(*, venue_symbol: str, side: str,
                         amount_quote: float = 0.0, amount_base: float = 0.0,
                         client_id: str = "", edge_mode: str = "dryrun", edge_hold: bool = False, **_):
    symbol = venue_symbol.replace("/", "-").upper()  # BTC/USDC → BTC-USDC
    side_uc = (side or "").upper()

    # SELL requires base qty; BUY requires quote amount
    if edge_mode == "live" and side_uc == "SELL" and not amount_base:
        return {"status":"error","message":"Coinbase MARKET SELL requires base amount (amount_base)","fills":[],
                "venue":"COINBASE","symbol":symbol,"side":side_uc}
    if edge_mode == "live" and side_uc == "BUY" and not amount_quote:
        return {"status":"error","message":"Coinbase MARKET BUY requires quote amount (amount_quote)","fills":[],
                "venue":"COINBASE","symbol":symbol,"side":side_uc}

    if edge_hold:
        return {"status":"held","message":"EDGE_HOLD enabled","fills":[],
                "venue":"COINBASE","symbol":symbol,"side":side_uc}

    if edge_mode != "live":
        px = 60000.0
        qty = round(float(amount_base or (amount_quote or 0.0)/px), 8)
        return {"status":"ok","txid":f"SIM-CB-{int(time.time()*1000)}","fills":[{"qty":qty,"price":px}],
                "venue":"COINBASE","symbol":symbol,"side":side_uc,"executed_qty":qty,"avg_price":px,
                "message":"coinbase dryrun simulated fill"}

    try:
        cb = CoinbaseCDP()
        resp = cb.place_market(product_id=symbol, side=side_uc,
                               quote_size=float(amount_quote or 0.0),
                               base_size=float(amount_base or 0.0),
                               client_order_id=str(client_id))
        try:
            data = resp.json()
        except Exception:
            data = {"raw": resp.text}

        if resp.status_code >= 400:
            return {"status":"error","message":f"{resp.status_code} {data}","fills":[],
                    "venue":"COINBASE","symbol":symbol,"side":side_uc}

        order_id = (data.get("success_response") or {}).get("order_id") or data.get("order_id") or ""
        return {"status":"ok","txid":order_id or f"CB-NOORD-{int(time.time()*1000)}","fills":data.get("fills") or [],
                "venue":"COINBASE","symbol":symbol,"side":side_uc,
                "message":"coinbase live order accepted" if order_id else "coinbase response parsed"}
    except Exception as e:
        return {"status":"error","message":f"coinbase executor exception: {e}","fills":[],
                "venue":"COINBASE","symbol":symbol,"side":side_uc}

 
