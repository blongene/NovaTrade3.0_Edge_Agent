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

    The Bus verifier typically HMACs the raw request body (request.get_data()).
    If we let requests/json serialize independently, minor whitespace / key-order
    differences can break signatures. We therefore:
      - sort_keys=True
      - compact separators
      - UTF-8 encoding
    """
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


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


# telemetry_sender.py — Edge -> Bus balances (push-on-boot + periodic + backoff)
import os
import json
import time
import hmac
import hashlib
import threading
from typing import Dict, Any, Tuple, Optional, List

import requests

# Base URL for the Bus (NovaTrade 3.0 cloud)
BUS_BASE = (os.getenv("CLOUD_BASE_URL") or os.getenv("BUS_BASE_URL", "") or "").rstrip("/")

# HMAC secret shared with the Bus (wsgi.py /api/telemetry/push_balances)
TELEM_SECRET = os.getenv("TELEMETRY_SECRET", "")

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

# Which assets to highlight in the log snapshot
HEADLINE = ("USDT", "USDC", "USD", "BTC", "ETH")

PUSH_IVL = int(os.getenv("PUSH_BALANCES_INTERVAL_SECS", "120"))
HEARTBEAT_ON_BOOT = (os.getenv("PUSH_BALANCES_ON_BOOT") or "1").lower() in {
    "1",
    "true",
    "yes",
    "on",
}
CACHE_PATH = os.getenv(
    "EDGE_LAST_BALANCES_PATH", os.path.expanduser("~/.nova/last_balances.json")
)

# Quotes we treat as “venue liquidity”
QUOTES = ("USD", "USDC", "USDT")


# --------------------------------------------------------------------------- #
# Canonical agent identity (trust boundary)
# --------------------------------------------------------------------------- #

def _stamp_agent(payload: dict) -> dict:
    """
    Force canonical agent identity onto any payload
    (including cache restores).
    """
    try:
        agent = _resolve_agent_id()
        payload = payload or {}
        payload["agent"] = _resolve_agent_id()
        payload["agent_id"] = _resolve_agent_id()
    except Exception:
        pass
    return payload

# --------------------------------------------------------------------------- #
# HMAC + HTTP
# --------------------------------------------------------------------------- #
def _hmac_sig(raw: bytes) -> str:
    if not TELEM_SECRET:
        return ""
    return hmac.new(TELEM_SECRET.encode(), raw, hashlib.sha256).hexdigest()


def _post_json(path: str, payload: dict) -> Tuple[int, str]:
    if not BUS_BASE:
        return 0, "BUS_BASE not configured"

    # Enforce canonical agent ID at send-time too (defense in depth)
    payload = _stamp_agent(payload)

    raw = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode()
    sig = _hmac_sig(raw)

    headers = {"Content-Type": "application/json"}
    if sig:
        # send both for compatibility with Bus
        headers["X-TELEMETRY-SIGN"] = sig
        headers["X-NT-Sig"] = sig

    url = f"{BUS_BASE}{path}"
    r = requests.post(url, data=raw, headers=headers, timeout=10)
    return r.status_code, r.text


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
        os.makedirs(os.path.dirname(CACHE_PATH), exist_ok=True)
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
    Normalizes to uppercase, strips blanks.
    """
    for nm in names:
        raw = (os.getenv(nm, "") or "").strip()
        if raw:
            return [x.strip().upper() for x in raw.split(",") if x.strip()]
    return []


def _required_venues(by_venue_raw: Dict[str, Any]) -> List[str]:
    """
    Stable required venues list:
      1) VENUE_ORDER (Edge explicit)
      2) TELEMETRY_REQUIRED_VENUES (Bus-style)
      3) ROUTER_ALLOWED / VENUES_ALLOWED (Edge router style)
      4) fallback default (keeps Bus happy)
    """
    venue_order = _env_list("VENUE_ORDER")
    if venue_order:
        return venue_order

    req = _env_list("TELEMETRY_REQUIRED_VENUES")
    if req:
        return req

    allowed = _env_list("ROUTER_ALLOWED", "ROUTER_ALLOWED_VENUES", "VENUES_ALLOWED")
    if allowed:
        return allowed

    # fallback (NovaTrade core 3)
    return ["BINANCEUS", "KRAKEN", "COINBASE"]


# --------------------------------------------------------------------------- #
# Balance collection
# --------------------------------------------------------------------------- #
def _collect_balances() -> dict:
    """
    Uses existing Edge executors to collect balances.

    Returns the Bus-friendly shape:
      {agent, by_venue: {VENUE:{USD:..,USDC:..,USDT:..}}, flat:{BTC:..,ETH:..,...}, ts, errors?}

    HARDENING (important):
      - Always includes required venues (VENUE_ORDER/TELEMETRY_REQUIRED_VENUES/ROUTER_ALLOWED)
      - Missing venues get zero-quote placeholders (truthful + prevents Bus gating)
      - Emits payload["errors"] with concise root cause keys if collection fails
      - Canonical agent identity is enforced (env-only)
    """
    agent = _resolve_agent_id()
    ts = int(time.time())

    errors: Dict[str, str] = {}

    # venue->asset balances (dict[str, dict[str,float]])
    by_venue_raw: Dict[str, Dict[str, Any]] = {}

    try:
        # Preferred: module that already gathers all venues
        from executors.binance_us_executor import get_balances as _edge_get_balances  # type: ignore

        by_venue_raw = _edge_get_balances() or {}
    except Exception as e:
        errors["BALANCE_COLLECT"] = str(e)[:220]

        # Fallback: try coinbase directly if available, and leave others out
        try:
            from executors.coinbase_advanced_executor import CoinbaseCDP  # type: ignore

            by_venue_raw["COINBASE"] = CoinbaseCDP().balances()
        except Exception as ee:
            errors["COINBASE_FALLBACK"] = str(ee)[:220]

        if TELEM_DEBUG:
            print(f"[telemetry] get_balances() error; using COINBASE-only fallback: {e}")

    # Optional debug: show what we actually got back per venue (quotes only)
    if TELEM_DEBUG or TELEM_DEBUG_DUMP:
        try:
            dbg_bits = []
            for venue, assets in (by_venue_raw or {}).items():
                if not isinstance(assets, dict):
                    continue
                pieces = []
                for q in QUOTES:
                    if q in assets:
                        try:
                            pieces.append(f"{q}={float(assets[q] or 0):.6f}")
                        except Exception:
                            pieces.append(f"{q}=?")
                dbg_bits.append(f"{venue}:{','.join(pieces) or 'no-quotes'}")
            if dbg_bits:
                print(f"[telemetry] raw balances venues={len(by_venue_raw)} " + " | ".join(dbg_bits))
            else:
                print("[telemetry] raw balances EMPTY from get_balances()")
        except Exception as e:
            errors["DEBUG_SUMMARY"] = str(e)[:220]
            print(f"[telemetry] debug summary failed: {e}")

    # Normalize into by_venue (only quote assets) and flat (sum of base assets across venues)
    by_venue: Dict[str, Dict[str, float]] = {}
    flat: Dict[str, float] = {}

    for venue, assets in (by_venue_raw or {}).items():
        if not isinstance(assets, dict):
            continue

        vkey = (venue or "").upper().strip()
        if not vkey:
            continue

        v_out: Dict[str, float] = {}

        for asset, amt in assets.items():
            try:
                a = (asset or "").upper().strip()
                x = float(amt or 0.0)
            except Exception:
                continue

            if not a:
                continue

            if a in QUOTES:
                v_out[a] = v_out.get(a, 0.0) + x
            else:
                flat[a] = flat.get(a, 0.0) + x

        # Keep quote balances if present (otherwise we may still add placeholders below)
        if v_out:
            by_venue[vkey] = v_out

    # Ensure required venues are present (zero placeholders are OK / truthful)
    required = _required_venues(by_venue_raw)

    for v in required:
        v = (v or "").upper().strip()
        if not v:
            continue

        if v not in by_venue:
            by_venue[v] = {q: 0.0 for q in QUOTES}
        else:
            for q in QUOTES:
                by_venue[v].setdefault(q, 0.0)

    payload: Dict[str, Any] = {"agent": agent, "by_venue": by_venue, "flat": flat, "ts": ts}
    if errors:
        payload["errors"] = errors

    # Enforce canonical agent once more
    payload = _stamp_agent(payload)
    return payload


# --------------------------------------------------------------------------- #
# Human-readable summary for Edge logs
# --------------------------------------------------------------------------- #
def _summarize_snapshot(payload: dict) -> str:
    """
    Build a compact human-readable snapshot for logs.

    Example:
        agent=edge-primary BINANCEUS:USDT=123.45,USD=10.00 | COINBASE:USDC=19.30 errors=COINBASE_FALLBACK || flat:BTC=0.015
    """
    try:
        # Env-only identity (never payload override)
        agent = _resolve_agent_id()

        by_venue = payload.get("by_venue") or {}
        flat = payload.get("flat") or {}
        errs = payload.get("errors") or {}

        parts = []
        for venue, amap in sorted(by_venue.items()):
            if not isinstance(amap, dict):
                continue
            bits = []
            for asset in HEADLINE:
                if asset in amap:
                    try:
                        bits.append(f"{asset}={float(amap[asset]):.2f}")
                    except Exception:
                        pass
            label = venue
            if bits:
                parts.append(f"{label}:" + ",".join(bits))
            else:
                parts.append(f"{label}:<no headline quotes>")

        flat_bits = []
        for asset in HEADLINE:
            if asset in flat:
                try:
                    flat_bits.append(f"{asset}={float(flat[asset]):.4f}")
                except Exception:
                    pass

        tail = ""
        if flat_bits:
            tail = " || flat:" + ",".join(flat_bits)

        err_tail = ""
        if isinstance(errs, dict) and errs:
            try:
                err_tail = " errors=" + ",".join(sorted([str(k) for k in errs.keys()]))
            except Exception:
                err_tail = " errors=1"

        core = " | ".join(parts) if parts else "no venues"
        return f"agent={agent} {core}{err_tail}{tail}"
    except Exception as e:
        return f"(summary-error: {e})"


# --------------------------------------------------------------------------- #
# Push + loop
# --------------------------------------------------------------------------- #
def push_balances_once(use_cache_if_empty: bool = True) -> bool:
    payload = _collect_balances()

    # If empty and we have a cache, send cached so Bus has *something* after deploys
    if use_cache_if_empty and not payload.get("by_venue") and not payload.get("flat"):
        cached = _load_cache()
        if cached:
            payload = cached

    # Always stamp canonical agent (prevents cached legacy agent ids)
    payload = _stamp_agent(payload)

    # Always emit a compact snapshot line so Edge logs show balances evolution.
    if TELEM_DEBUG or TELEM_DEBUG_DUMP:
        try:
            summary = _summarize_snapshot(payload)
            print(f"[telemetry] snapshot {summary}")
            if TELEM_DEBUG_DUMP:
                try:
                    dumped = json.dumps(
                        payload,
                        sort_keys=True,
                        separators=(",", ":"),
                        ensure_ascii=False,
                    )
                    if len(dumped) > 2000:
                        dumped = dumped[:2000] + "...(truncated)"
                    print(f"[telemetry] payload={dumped}")
                except Exception as e:
                    print(f"[telemetry] payload-dump-error: {e}")
        except Exception as e:
            print(f"[telemetry] snapshot-error: {e}")

    ok = False
    for attempt in (1, 2, 3):
        try:
            code, text = _post_json("/api/telemetry/push_balances", payload)
            ok = 200 <= code < 300
            if ok:
                _save_cache(payload)
                if TELEM_DEBUG:
                    print(f"[telemetry] push_balances ok (attempt {attempt})")
                break
            else:
                print(f"[telemetry] push_balances failed {code}: {text}")
        except Exception as e:
            print(f"[telemetry] error: {e}")
        time.sleep(2**attempt)  # 2s, 4s backoff
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


# --------------------------------------------------------------------------- #
# CLI / module entrypoint
# --------------------------------------------------------------------------- #
def main() -> int:
    ok = push_balances_once(use_cache_if_empty=True)
    # Keep this short and grep-friendly
    print(f"[telemetry] one-shot push ok={ok} agent={_resolve_agent_id()}")
    return 0 if ok else 2


if __name__ == "__main__":
    raise SystemExit(main())


def _post_json(path: str, payload: dict):
    """
    POST signed JSON to Bus, using deterministic serialization so HMAC matches.
    """
    base = (os.getenv("CLOUD_BASE_URL") or os.getenv("BUS_BASE_URL") or os.getenv("BASE_URL") or "").rstrip("/")
    if not base:
        raise RuntimeError("CLOUD_BASE_URL/BUS_BASE_URL/BASE_URL not set")
    url = base + path

    secret = TELEM_SECRET
    if not secret:
        raise RuntimeError("No telemetry signing secret set (OUTBOX_SECRET/EDGE_SECRET/TELEMETRY_SECRET).")

    body = _canonical_json_bytes(payload)
    headers = _auth_headers(secret, body)

    resp = requests.post(url, data=body, headers=headers, timeout=REQ_TIMEOUT)
    return resp.status_code, resp.text
