# telemetry_sender.py — Edge -> Bus balances (push-on-boot + periodic + backoff)
import os, json, time, hmac, hashlib, threading, requests
from datetime import datetime

BUS_BASE = os.getenv("CLOUD_BASE_URL") or os.getenv("BUS_BASE_URL")
TELEM_SECRET = os.getenv("TELEMETRY_SECRET", "")
PUSH_IVL = int(os.getenv("PUSH_BALANCES_INTERVAL_SECS", "120"))
HEARTBEAT_ON_BOOT = os.getenv("PUSH_BALANCES_ON_BOOT", "1").lower() in {"1","true","yes","on"}
CACHE_PATH = os.getenv("EDGE_LAST_BALANCES_PATH", os.path.expanduser("~/.nova/last_balances.json"))

def _hmac_sig(raw: bytes) -> str:
    return hmac.new(TELEM_SECRET.encode(), raw, hashlib.sha256).hexdigest()

def _post_json(url: str, payload: dict) -> tuple[int, str]:
    raw = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode()
    sig = _hmac_sig(raw)
    r = requests.post(
        f"{BUS_BASE}{url}",
        data=raw,
        headers={"Content-Type": "application/json", "X-NT-Sig": sig},
        timeout=10,
    )
    return r.status_code, r.text

def _load_cache():
    try:
        with open(CACHE_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None

def _save_cache(obj: dict):
    try:
        os.makedirs(os.path.dirname(CACHE_PATH), exist_ok=True)
        with open(CACHE_PATH, "w", encoding="utf-8") as f:
            json.dump(obj, f, separators=(",", ":"))
    except Exception:
        pass

def _collect_balances() -> dict:
    """Replace with your real balance collector; this is a simple stub."""
    # expected shape: {agent, by_venue, flat, ts}
    agent = os.getenv("EDGE_AGENT_ID") or os.getenv("AGENT_ID") or "edge"
    # TODO: wire your actual venue balances here
    by_venue = {}  # e.g., {"COINBASE":{"USD":123.45}, "BINANCEUS":{"USDT":456.78}}
    flat = {}      # e.g., {"BTC": 0.12, "ETH": 1.0}
    ts = int(time.time())
    return {"agent": agent, "by_venue": by_venue, "flat": flat, "ts": ts}

def push_balances_once(use_cache_if_empty=True) -> bool:
    payload = _collect_balances()
    # if empty and we have a cache, send cached so Bus has *something* after deploys
    if use_cache_if_empty and not payload.get("by_venue") and not payload.get("flat"):
        cached = _load_cache()
        if cached:
            payload = cached
    ok = False
    for attempt in (1, 2, 3):
        try:
            code, text = _post_json("/api/telemetry/push_balances", payload)
            ok = (200 <= code < 300)
            if ok:
                _save_cache(payload)
                print(f"[telemetry] push_balances ok (attempt {attempt})")
                break
            else:
                print(f"[telemetry] push_balances failed {code}: {text}")
        except Exception as e:
            print(f"[telemetry] error: {e}")
        time.sleep(2 ** attempt)  # 2s, 4s backoff
    return ok

def start_balance_pusher():
    def loop():
        if HEARTBEAT_ON_BOOT:
            push_balances_once(use_cache_if_empty=True)
        while True:
            start = time.time()
            push_balances_once(use_cache_if_empty=False)
            slp = max(5, PUSH_IVL - int(time.time() - start))
            time.sleep(slp)
    t = threading.Thread(target=loop, name="balance-pusher", daemon=True)
    t.start()
    return t
