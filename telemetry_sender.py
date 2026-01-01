# telemetry_sender.py — Edge -> Bus balances (push-on-boot + periodic + backoff)
# Production-grade, compatibility-safe drop-in replacement.
#
# Key properties:
# - Maintains Edge import contract: _collect_balances(), push_balances_once(), start_balance_pusher()
# - Single canonical _post_json() (fixes "double definition override" bug)
# - Deterministic JSON signing bytes (sorted keys + compact separators)
# - Secret tolerance: TELEMETRY_SECRET -> EDGE_SECRET -> OUTBOX_SECRET
# - Compatibility headers: X-OUTBOX-SIGN, X-TELEMETRY-SIGN, X-Nova-Signature, X-NOVA-SIGNATURE, X-NOVA-SIGN, X-NT-Sig
# - Required venues always present with truthful zero placeholders for quotes
# - Cache mkdir safe even if path has no directory
# - Does not crash Edge on network failures (best-effort push)

import os
import json
import time
import hmac
import hashlib
import threading
from typing import Dict, Any, Tuple, Optional, List

import requests


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


def _resolve_signing_secret() -> str:
    """
    Prefer TELEMETRY_SECRET for telemetry endpoints, but tolerate EDGE_SECRET / OUTBOX_SECRET
    for phased migrations or mixed deployments.
    """
    return (os.getenv("TELEMETRY_SECRET") or os.getenv("EDGE_SECRET") or os.getenv("OUTBOX_SECRET") or "").strip()


def _auth_headers_for_sig(sig: str) -> dict:
    """
    Compatibility: Bus has historically used different header names.
    We send the same signature under multiple headers, with X-OUTBOX-SIGN as primary.
    """
    return {
        "Content-Type": "application/json",
        "X-OUTBOX-SIGN": sig,        # Phase 24/25 canonical
        "X-TELEMETRY-SIGN": sig,     # legacy/older variants
        "X-Nova-Signature": sig,
        "X-NOVA-SIGNATURE": sig,
        "X-NOVA-SIGN": sig,
        "X-NT-Sig": sig,
    }


# --------------------------------------------------------------------------- #
# Config
# --------------------------------------------------------------------------- #
BUS_BASE = (os.getenv("CLOUD_BASE_URL") or os.getenv("BUS_BASE_URL") or os.getenv("BASE_URL") or "").rstrip("/")

# Telemetry snapshot logging controls
TELEM_DEBUG = (os.getenv("TELEMETRY_SUMMARY_ENABLED") or "1").lower() in {"1", "true", "yes", "on"}
TELEM_DEBUG_DUMP = (os.getenv("TELEMETRY_DEBUG_DUMP") or "0").lower() in {"1", "true", "yes", "on"}

# Which assets to highlight in the log snapshot
HEADLINE = ("USDT", "USDC", "USD", "BTC", "ETH")

# Interval knobs (support both env names: older + newer)
PUSH_IVL = int(os.getenv("PUSH_BALANCES_EVERY_S") or os.getenv("PUSH_BALANCES_INTERVAL_SECS") or "600")
PUSH_ON_BOOT = (os.getenv("PUSH_BALANCES_ON_BOOT") or "1").lower() in {"1", "true", "yes", "on"}
PUSH_ENABLED = (os.getenv("PUSH_BALANCES_ENABLED") or "0").lower() in {"1", "true", "yes", "on"}

CACHE_PATH = os.getenv("EDGE_LAST_BALANCES_PATH", os.path.expanduser("~/.nova/last_balances.json"))

# Quotes we treat as “venue liquidity”
QUOTES = ("USD", "USDC", "USDT")

# Network / request timeouts
REQ_TIMEOUT = float(os.getenv("TELEMETRY_REQ_TIMEOUT", "10"))

# Optional allow-lists (do not enforce by default)
PUSH_VENUES = os.getenv("PUSH_VENUES", "").strip()
PUSH_ASSETS = os.getenv("PUSH_ASSETS", "").strip()


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
# Canonical agent identity (trust boundary)
# --------------------------------------------------------------------------- #
def _stamp_agent(payload: dict) -> dict:
    """
    Force canonical agent identity onto any payload (including cache restores).
    """
    try:
        agent = _resolve_agent_id()
        payload = payload or {}
        payload["agent"] = agent
        payload["agent_id"] = agent
    except Exception:
        pass
    return payload


# --------------------------------------------------------------------------- #
# Cache helpers (so Bus always has *something* after deploys)
# --------------------------------------------------------------------------- #
def _load_cache() -> Optional[dict]:
    try:
        with open(CACHE_PATH, "r", encoding="utf-8") as f:
            obj = json.load(f)
            return obj if isinstance(obj, dict) else None
    except Exception:
        return None


def _save_cache(obj: dict) -> None:
    try:
        d = os.path.dirname(CACHE_PATH)
        if d:
            os.makedirs(d, exist_ok=True)
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

    return ["BINANCEUS", "KRAKEN", "COINBASE"]


def _filter_allowlists(by_venue: Dict[str, Dict[str, float]]) -> Dict[str, Dict[str, float]]:
    """
    Optional allowlists: PUSH_VENUES / PUSH_ASSETS.
    If not set, returns original.
    """
    venues_allow = set(_env_list("PUSH_VENUES")) if PUSH_VENUES else set()
    assets_allow = set(_env_list("PUSH_ASSETS")) if PUSH_ASSETS else set()

    if not venues_allow and not assets_allow:
        return by_venue

    out: Dict[str, Dict[str, float]] = {}
    for v, amap in (by_venue or {}).items():
        vv = (v or "").upper().strip()
        if venues_allow and vv not in venues_allow:
            continue
        if not isinstance(amap, dict):
            continue
        kept: Dict[str, float] = {}
        for a, x in amap.items():
            aa = (a or "").upper().strip()
            if assets_allow and aa not in assets_allow:
                continue
            try:
                kept[aa] = float(x or 0.0)
            except Exception:
                kept[aa] = 0.0
        if kept:
            out[vv] = kept
    return out


# --------------------------------------------------------------------------- #
# HMAC + HTTP
# --------------------------------------------------------------------------- #
def _post_json(path: str, payload: dict) -> Tuple[int, str]:
    """
    POST signed JSON to Bus, using deterministic serialization so HMAC matches.

    IMPORTANT:
      - never raises (to avoid crashing Edge)
      - uses secret tolerance
      - stamps canonical agent id
      - sends signature under multiple header names
    """
    base = (os.getenv("CLOUD_BASE_URL") or os.getenv("BUS_BASE_URL") or os.getenv("BASE_URL") or "").rstrip("/")
    if not base:
        return 0, "BUS_BASE not configured (CLOUD_BASE_URL/BUS_BASE_URL/BASE_URL)"

    secret = _resolve_signing_secret()
    if not secret:
        return 0, "No signing secret set (TELEMETRY_SECRET/EDGE_SECRET/OUTBOX_SECRET)"

    payload = _stamp_agent(payload)
    body = _canonical_json_bytes(payload)
    sig = _hmac_sha256_hex(secret, body)
    headers = _auth_headers_for_sig(sig)

    url = f"{base}{path}"
    try:
        resp = requests.post(url, data=body, headers=headers, timeout=REQ_TIMEOUT)
        return resp.status_code, resp.text
    except Exception as e:
        return 0, f"POST failed: {e}"


# --------------------------------------------------------------------------- #
# Balance collection
# --------------------------------------------------------------------------- #
def _collect_balances() -> dict:
    """
    Uses existing Edge executors to collect balances.

    Returns the Bus-friendly shape:
      {agent, agent_id, by_venue: {VENUE:{USD:..,USDC:..,USDT:..}}, flat:{BTC:..,ETH:..,...}, ts, errors?}

    HARDENING:
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

        _log(f"get_balances() error; using COINBASE-only fallback: {e}")

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

    # Normalize into by_venue (only quote assets) and flat (sum of non-quote assets across venues)
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

    # Optional allowlists (do not break required-venue placeholders)
    by_venue = _filter_allowlists(by_venue)

    payload: Dict[str, Any] = {"agent": agent, "by_venue": by_venue, "flat": flat, "ts": ts}
    if errors:
        payload["errors"] = errors

    return _stamp_agent(payload)


# --------------------------------------------------------------------------- #
# Human-readable summary for Edge logs
# --------------------------------------------------------------------------- #
def _summarize_snapshot(payload: dict) -> str:
    """
    Build a compact human-readable snapshot for logs.

    Example:
        agent=edge-primary BINANCEUS:USDT=123.45,USD=10.00 | COINBASE:USDC=19.30 errors=BALANCE_COLLECT || flat:BTC=0.015
    """
    try:
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
            parts.append(f"{venue}:" + (",".join(bits) if bits else "<no headline>"))

        flat_bits = []
        for asset in HEADLINE:
            if asset in flat:
                try:
                    flat_bits.append(f"{asset}={float(flat[asset]):.4f}")
                except Exception:
                    pass

        tail = (" || flat:" + ",".join(flat_bits)) if flat_bits else ""

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
    """
    Push balances to Bus once. Best-effort: never raises.
    Writes cache on successful push.
    """
    payload = _collect_balances()

    # If empty and we have a cache, send cached so Bus has *something* after deploys
    if use_cache_if_empty and not payload.get("by_venue") and not payload.get("flat"):
        cached = _load_cache()
        if cached and isinstance(cached, dict):
            payload = cached

    payload = _stamp_agent(payload)

    # Always emit a compact snapshot line so Edge logs show balances evolution.
    if TELEM_DEBUG or TELEM_DEBUG_DUMP:
        try:
            summary = _summarize_snapshot(payload)
            print(f"[telemetry] snapshot {summary}")
            if TELEM_DEBUG_DUMP:
                try:
                    dumped = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
                    if len(dumped) > 2000:
                        dumped = dumped[:2000] + "...(truncated)"
                    print(f"[telemetry] payload={dumped}")
                except Exception as e:
                    print(f"[telemetry] payload-dump-error: {e}")
        except Exception as e:
            print(f"[telemetry] snapshot-error: {e}")

    ok = False
    # bounded retry/backoff
    for attempt in (1, 2, 3):
        try:
            code, text = _post_json("/api/telemetry/push_balances", payload)
            ok = 200 <= int(code or 0) < 300
            if ok:
                _save_cache(payload)
                _log(f"push_balances ok (attempt {attempt})")
                break
            else:
                print(f"[telemetry] push_balances failed {code}: {(text or '')[:300]}")
        except Exception as e:
            # should be rare because _post_json never raises, but keep best-effort behavior
            print(f"[telemetry] error: {e}")
        time.sleep(2 ** attempt)  # 2s, 4s, 8s backoff
    return ok


def start_balance_pusher():
    """
    Start background loop.
    Compatibility: callable with no args.
    Honors:
      PUSH_BALANCES_ENABLED (if set; otherwise starts like legacy behavior)
      PUSH_BALANCES_EVERY_S / PUSH_BALANCES_INTERVAL_SECS
      PUSH_BALANCES_ON_BOOT
    """
    def loop():
        if PUSH_ON_BOOT:
            push_balances_once(use_cache_if_empty=True)

        # If PUSH_BALANCES_ENABLED is explicitly disabled, stay dormant after boot.
        # This avoids surprising behavior, but preserves legacy boot snapshot if desired.
        if os.getenv("PUSH_BALANCES_ENABLED") is not None and not PUSH_ENABLED:
            _log("PUSH_BALANCES_ENABLED=0; exiting balance pusher loop after boot snapshot")
            return

        while True:
            start = time.time()
            push_balances_once(use_cache_if_empty=False)
            slp = max(5, int(PUSH_IVL) - int(time.time() - start))
            time.sleep(slp)

    t = threading.Thread(target=loop, name="balance-pusher", daemon=True)
    t.start()
    return t


# --------------------------------------------------------------------------- #
# CLI / module entrypoint
# --------------------------------------------------------------------------- #
def main() -> int:
    ok = push_balances_once(use_cache_if_empty=True)
    print(f"[telemetry] one-shot push ok={ok} agent={_resolve_agent_id()}")
    return 0 if ok else 2


if __name__ == "__main__":
    raise SystemExit(main())
