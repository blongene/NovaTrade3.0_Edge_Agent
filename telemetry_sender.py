# telemetry_sender.py — Edge -> Bus balances (push-on-boot + periodic + backoff)
import os
import json
import time
import hmac
import hashlib
import threading
from typing import Dict, Any, Tuple, Optional, List

import requests


def _hmac_sha256_hex(secret: str, body_bytes: bytes) -> str:
    return hmac.new(secret.encode("utf-8"), body_bytes, hashlib.sha256).hexdigest()


def _auth_headers(secret: str, body_bytes: bytes) -> dict:
    """
    Compatibility: Bus has historically used different header names.
    We send the same signature under multiple headers, with X-OUTBOX-SIGN as primary.
    """
    sig = _hmac_sha256_hex(secret, body_bytes)
    return {
        "Content-Type": "application/json",
        "X-OUTBOX-SIGN": sig,       # Phase 24/25 canonical
        "X-TELEMETRY-SIGN": sig,    # legacy/older variants
        "X-NOVA-SIGNATURE": sig,
        "X-NOVA-SIGN": sig,
    }


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def _resolve_agent_id() -> str:
    """
    Canonical agent id to use for telemetry.
    Priority: EDGE_AGENT_ID -> AGENT_ID -> AGENT -> "edge"
    """
    return (os.getenv("EDGE_AGENT_ID") or os.getenv("AGENT_ID") or os.getenv("AGENT") or "edge").strip() or "edge"


def _canonical_json_bytes(obj: dict) -> bytes:
    """
    IMPORTANT: We sign the *exact bytes* we send to the Bus.

    We serialize deterministically to avoid signature mismatch:
      - compact separators
      - sort_keys=True
      - ensure_ascii=False (preserve unicode)
    """
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def _stamp_agent(payload: dict) -> dict:
    """
    Ensure telemetry payload includes canonical agent id fields.
    """
    agent = _resolve_agent_id()
    out = dict(payload or {})
    out.setdefault("agent_id", agent)
    # Some Bus code expects 'agent' too; keep both for compatibility.
    out.setdefault("agent", agent)
    return out


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #

# Base URL for the Bus (NovaTrade 3.0 cloud)
BUS_BASE = (os.getenv("CLOUD_BASE_URL") or os.getenv("BUS_BASE_URL", "") or "").rstrip("/")

# HMAC secrets (tolerant selection happens at send-time)
TELEMETRY_SECRET = (os.getenv("TELEMETRY_SECRET") or "").strip()
EDGE_SECRET = (os.getenv("EDGE_SECRET") or "").strip()
OUTBOX_SECRET = (os.getenv("OUTBOX_SECRET") or "").strip()
# Prefer TELEMETRY_SECRET for telemetry endpoints; fall back to EDGE/OUTBOX for tolerance.
SIGNING_SECRET = TELEMETRY_SECRET or EDGE_SECRET or OUTBOX_SECRET

# Telemetry snapshot logging controls
TELEM_DEBUG = (os.getenv("TELEMETRY_SUMMARY_ENABLED") or "1").lower() in {
    "1",
    "true",
    "yes",
    "on",
}
TELEM_DEBUG_DUMP = (os.getenv("TELEMETRY_DEBUG_DUMP") or "0").lower() in {
    "1",
    "true",
    "yes",
    "on",
}

# Which venue assets we push (optional allowlist)
PUSH_VENUES = os.getenv("PUSH_VENUES", "").strip()  # e.g. "BINANCEUS,COINBASE,KRAKEN"
PUSH_ASSETS = os.getenv("PUSH_ASSETS", "").strip()  # e.g. "USD,USDT,USDC,BTC"

# Local cache path (so Bus always has *something* after deploys)
CACHE_PATH = os.getenv("EDGE_LAST_BALANCES_PATH", os.path.expanduser("~/.nova/last_balances.json"))

# Quotes we treat as “venue liquidity”
QUOTES = ("USD", "USDC", "USDT")

# Network / request timeouts
REQ_TIMEOUT = float(os.getenv("TELEMETRY_REQ_TIMEOUT", "10"))


# --------------------------------------------------------------------------- #
# Canonical agent identity (trust boundary)
# --------------------------------------------------------------------------- #
def _log(msg: str) -> None:
    if TELEM_DEBUG:
        print(f"[telemetry_sender] {msg}")


def _dump(obj: Any) -> None:
    if TELEM_DEBUG_DUMP:
        try:
            print("[telemetry_sender] dump:", json.dumps(obj, indent=2, sort_keys=True)[:4000])
        except Exception:
            pass


# --------------------------------------------------------------------------- #
# Cache helpers (so Bus always has *something* after deploys)
# --------------------------------------------------------------------------- #
def _load_cache() -> Optional[dict]:
    try:
        with open(CACHE_PATH, "r", encoding="utf-8") as f:
            obj = json.load(f)
            if isinstance(obj, dict):
                return obj
    except Exception:
        return None
    return None


def _save_cache(obj: dict) -> None:
    try:
        # Avoid makedirs("") if a bare filename is provided
        (_d := os.path.dirname(CACHE_PATH)) and os.makedirs(_d, exist_ok=True)
        with open(CACHE_PATH, "w", encoding="utf-8") as f:
            json.dump(obj, f, separators=(",", ":"))
    except Exception:
        # Cache is best-effort only
        pass


# --------------------------------------------------------------------------- #
# Env helpers
# --------------------------------------------------------------------------- #
def _env_list(*names: str) -> List[str]:
    """
    Return first non-empty comma-separated env list among names.
    Normalizes to uppercased trimmed values.
    """
    for n in names:
        raw = (os.getenv(n) or "").strip()
        if raw:
            return [x.strip().upper() for x in raw.split(",") if x.strip()]
    return []


def _filter_balances(bals: Dict[str, Dict[str, float]]) -> Dict[str, Dict[str, float]]:
    """
    Apply optional allowlists from env.
    """
    venues_allow = set(_env_list("PUSH_VENUES", "TELEMETRY_VENUES"))
    assets_allow = set(_env_list("PUSH_ASSETS", "TELEMETRY_ASSETS"))

    out: Dict[str, Dict[str, float]] = {}
    for venue, mp in (bals or {}).items():
        v = (venue or "").upper()
        if venues_allow and v not in venues_allow:
            continue
        if not isinstance(mp, dict):
            continue
        filtered: Dict[str, float] = {}
        for asset, amt in mp.items():
            a = (asset or "").upper()
            if assets_allow and a not in assets_allow:
                continue
            try:
                filtered[a] = float(amt or 0.0)
            except Exception:
                filtered[a] = 0.0
        if filtered:
            out[v] = filtered
    return out


# --------------------------------------------------------------------------- #
# HMAC + HTTP
# --------------------------------------------------------------------------- #
def _resolve_bus_base() -> str:
    return (os.getenv("CLOUD_BASE_URL") or os.getenv("BUS_BASE_URL") or os.getenv("BASE_URL") or "").rstrip("/")


def _resolve_signing_secret() -> str:
    # Prefer TELEMETRY_SECRET for telemetry endpoints, but tolerate EDGE/OUTBOX for
    # backwards compatibility / phased migrations.
    return (os.getenv("TELEMETRY_SECRET") or os.getenv("EDGE_SECRET") or os.getenv("OUTBOX_SECRET") or "").strip()


def _post_json(path: str, payload: dict) -> Tuple[int, str]:
    """POST signed JSON to Bus with deterministic serialization and compatible headers.

    - Uses canonical JSON bytes for signing (sort_keys=True, compact separators)
    - Sends the signature under multiple header names the Bus may accept
    - Returns (status_code, response_text) and never raises
    """
    base = _resolve_bus_base()
    if not base:
        return 0, "BUS_BASE not configured (CLOUD_BASE_URL/BUS_BASE_URL/BASE_URL)"

    secret = _resolve_signing_secret()
    if not secret:
        return 0, "No signing secret set (TELEMETRY_SECRET/EDGE_SECRET/OUTBOX_SECRET)"

    # Enforce canonical agent ID at send-time too (defense in depth)
    payload = _stamp_agent(payload)

    body = _canonical_json_bytes(payload)
    headers = _auth_headers(secret, body)

    url = f"{base}{path}"
    try:
        r = requests.post(url, data=body, headers=headers, timeout=REQ_TIMEOUT)
        return r.status_code, r.text
    except Exception as e:
        return 0, f"POST failed: {e}"


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
def build_balances_payload(
    balances_by_venue: Dict[str, Dict[str, float]],
    source: str = "edge",
    extra: Optional[dict] = None,
) -> dict:
    """
    Build telemetry payload expected by Bus /api/telemetry/push_balances.
    """
    payload = {
        "ts": int(time.time()),
        "source": source,
        "balances": _filter_balances(balances_by_venue or {}),
    }
    if extra and isinstance(extra, dict):
        payload.update(extra)
    return _stamp_agent(payload)


def push_balances_once(
    balances_by_venue: Optional[Dict[str, Dict[str, float]]] = None,
    use_cache_if_empty: bool = True,
    path: str = "/api/telemetry/push_balances",
) -> bool:
    """
    Push balances to Bus once. If balances_by_venue is None/empty and use_cache_if_empty,
    use cached last balances if present.
    """
    bals = balances_by_venue or {}

    if (not bals) and use_cache_if_empty:
        cached = _load_cache()
        if cached and isinstance(cached.get("balances"), dict):
            bals = cached.get("balances") or {}
            _log("using cached balances (empty live input)")

    payload = build_balances_payload(bals)

    # Cache what we're about to send, best-effort.
    try:
        _save_cache({"ts": payload.get("ts"), "agent_id": payload.get("agent_id"), "balances": payload.get("balances")})
    except Exception:
        pass

    _dump(payload)

    code, text = _post_json(path, payload)
    ok = (200 <= int(code or 0) < 300)
    if ok:
        _log(f"push ok ({code})")
    else:
        _log(f"push failed ({code}): {text[:300] if isinstance(text,str) else text}")
    return ok


# --------------------------------------------------------------------------- #
# Background loop
# --------------------------------------------------------------------------- #
_STOP = threading.Event()
_THREAD: Optional[threading.Thread] = None


def start_balance_pusher(
    get_balances_fn,
    every_s: int = 600,
    push_on_boot: bool = True,
    path: str = "/api/telemetry/push_balances",
) -> None:
    """
    Start a background thread that periodically pushes balances to the Bus.

    get_balances_fn: callable -> Dict[str, Dict[str, float]]
    """
    global _THREAD
    if _THREAD and _THREAD.is_alive():
        return

    every_s = max(60, int(every_s or 600))
    push_on_boot = bool(push_on_boot)

    def _loop():
        if push_on_boot:
            try:
                bals = get_balances_fn() or {}
                push_balances_once(bals, use_cache_if_empty=True, path=path)
            except Exception as e:
                _log(f"boot push error: {e}")

        while not _STOP.is_set():
            try:
                bals = get_balances_fn() or {}
                push_balances_once(bals, use_cache_if_empty=True, path=path)
            except Exception as e:
                _log(f"push loop error: {e}")
            # sleep in small chunks so stop() is responsive
            for _ in range(every_s):
                if _STOP.is_set():
                    break
                time.sleep(1)

    _THREAD = threading.Thread(target=_loop, name="balance_pusher", daemon=True)
    _THREAD.start()
    _log(f"balance pusher started every_s={every_s} push_on_boot={push_on_boot}")


def stop_balance_pusher() -> None:
    _STOP.set()
    t = _THREAD
    if t and t.is_alive():
        try:
            t.join(timeout=5)
        except Exception:
            pass


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #
def main() -> int:
    """
    One-shot push (useful for smoke tests):
      python telemetry_sender.py
    """
    ok = push_balances_once(use_cache_if_empty=True)
    # Keep this short and grep-friendly
    print(f"[telemetry] one-shot push ok={ok} agent={_resolve_agent_id()}")
    return 0 if ok else 2


if __name__ == "__main__":
    raise SystemExit(main())
