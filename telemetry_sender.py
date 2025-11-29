# telemetry_sender.py — Edge -> Bus balances (push-on-boot + periodic + backoff)
import os
import json
import time
import hmac
import hashlib
import threading
from datetime import datetime

import requests

# Base URL for the Bus (NovaTrade 3.0 cloud)
BUS_BASE = os.getenv("CLOUD_BASE_URL") or os.getenv("BUS_BASE_URL", "").rstrip("/")

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
HEARTBEAT_ON_BOOT = os.getenv("PUSH_BALANCES_ON_BOOT", "1").lower() in {
    "1",
    "true",
    "yes",
    "on",
}
CACHE_PATH = os.getenv(
    "EDGE_LAST_BALANCES_PATH", os.path.expanduser("~/.nova/last_balances.json")
)


# --------------------------------------------------------------------------- #
# HMAC + HTTP
# --------------------------------------------------------------------------- #
def _hmac_sig(raw: bytes) -> str:
    if not TELEM_SECRET:
        return ""
    return hmac.new(TELEM_SECRET.encode(), raw, hashlib.sha256).hexdigest()


def _post_json(path: str, payload: dict) -> tuple[int, str]:
    if not BUS_BASE:
        return 0, "BUS_BASE not configured"

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
        # Cache is best-effort only
        pass


# --------------------------------------------------------------------------- #
# Balance collection
# --------------------------------------------------------------------------- #
def _collect_balances() -> dict:
    """
    Uses existing Edge executors to collect balances:

      - executors.binance_us_executor.get_balances()
            -> {"COINBASE": {...}, "BINANCEUS": {...}, "KRAKEN": {...}}

        (which itself uses CoinbaseCDP / BinanceUS / Kraken)

    Returns the Bus-friendly shape:

      {
        "agent": str,
        "by_venue": {VENUE: {"USD":..,"USDC":..,"USDT":..}},  # quote-only per venue
        "flat":    {ASSET: total_across_venues},             # all non-quote assets
        "ts":      int,
      }

    NOTE: Even if a venue only holds non-quote assets (alts), it will still
    appear in by_venue with an *empty* dict so the Bus log line includes
    that venue in its "venues=[...]" list.
    """
    agent = os.getenv("EDGE_AGENT_ID") or os.getenv("AGENT_ID") or "edge"
    ts = int(time.time())

    # --- pull the venue->asset balances (dict[str, dict[str,float]])
    by_venue_raw: dict[str, dict] = {}

    # Primary path: shared get_balances helper (preferred)
    try:
        from executors.binance_us_executor import (  # type: ignore
            get_balances as _edge_get_balances,
        )

        by_venue_raw = _edge_get_balances() or {}
    except Exception as e:
        print(f"[telemetry] get_balances error: {e}")

    # Fallback: if BINANCEUS missing but class is available, query it directly
    if "BINANCEUS" not in by_venue_raw:
        try:
            from executors.binance_us_executor import BinanceUS  # type: ignore

            bu = BinanceUS()
            acct = bu.account() or {}
            bals = {
                (b.get("asset") or "").upper(): float(b.get("free") or 0.0)
                for b in (acct.get("balances") or [])
            }
            if bals:
                by_venue_raw["BINANCEUS"] = bals
        except Exception as e:
            print(f"[telemetry] BINANCEUS fallback balances error: {e}")

    # --- normalize into by_venue (only quote assets) and flat (sum of base assets)
    quotes = ("USD", "USDC", "USDT")
    by_venue: dict[str, dict[str, float]] = {}
    flat: dict[str, float] = {}

    for venue, assets in (by_venue_raw or {}).items():
        if not isinstance(assets, dict):
            continue

        vkey = (venue or "").upper()
        v_out: dict[str, float] = {}
        has_any_asset = False  # track if this venue had *any* balances at all

        for asset, amt in assets.items():
            try:
                a = (asset or "").upper()
                x = float(amt or 0.0)
            except Exception:
                continue

            if x != 0.0:
                has_any_asset = True

            if a in quotes:
                v_out[a] = v_out.get(a, 0.0) + x
            else:
                flat[a] = flat.get(a, 0.0) + x

        # Keep the venue if it has any balances at all, even if no quote assets.
        # This ensures BINANCEUS still shows up in the Bus snapshot line when it
        # only holds alts.
        if v_out or has_any_asset:
            by_venue[vkey] = v_out

    # Optional additional hint if some venues disappeared after quote-only filter
    missing_venues = set((by_venue_raw or {}).keys()) - set(by_venue.keys())
    if missing_venues:
        print(
            f"[telemetry] venues with only zero balances (skipped in by_venue): "
            f"{','.join(sorted(missing_venues))}"
        )

    return {"agent": agent, "by_venue": by_venue, "flat": flat, "ts": ts}


# --------------------------------------------------------------------------- #
# Human-readable summary for Edge logs
# --------------------------------------------------------------------------- #
def _summarize_snapshot(payload: dict) -> str:
    """
    Build a compact human-readable snapshot for logs.

    Example:
        agent=edge-primary BINANCEUS:USDT=123.45,USD=10.00 | COINBASE:USDC=19.30 || flat:BTC=0.015
    """
    try:
        agent = payload.get("agent") or payload.get("agent_id") or "edge"
        by_venue = payload.get("by_venue") or {}
        flat = payload.get("flat") or {}

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
            # Show venue name even if no headline quote assets (alts-only venue)
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

        core = " | ".join(parts) if parts else "no venues"
        return f"agent={agent} {core}{tail}"
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
