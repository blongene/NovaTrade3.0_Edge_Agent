#!/usr/bin/env python3
"""
telemetry_sender.py (Edge)

Pushes balance snapshots to the Bus telemetry endpoint.

Key rules:
- Agent identity is ENV-driven (EDGE_AGENT_ID -> AGENT_ID -> "edge").
- Payload agent fields are stamped to the canonical env agent to prevent drift.
- Default behavior is DAEMON mode (loop forever); one-shot supported via env.

Env:
- BUS_BASE_URL / CLOUD_BASE_URL: Bus base URL
- EDGE_SECRET: HMAC secret (must match Bus OUTBOX_SECRET if Bus verifies telemetry using that secret)
- EDGE_AGENT_ID / AGENT_ID: canonical edge agent id
- TELEMETRY_INTERVAL_SEC: loop interval seconds (default 60)
- TELEMETRY_ONESHOT: set "1" to do a single push and exit
- TELEMETRY_DEBUG / TELEMETRY_DEBUG_DUMP: verbose logging
- TELEMETRY_HEARTBEAT_ON_BOOT: push immediately when daemon starts (default 1)
"""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import signal
import threading
import time
from typing import Any, Dict, Optional, Tuple

import requests

# --------------------------------------------------------------------------- #
# Config
# --------------------------------------------------------------------------- #

BUS_BASE_URL = (
    (os.getenv("BUS_BASE_URL") or "").strip()
    or (os.getenv("CLOUD_BASE_URL") or "").strip()
    or (os.getenv("BASE_URL") or "").strip()
).rstrip("/")

EDGE_SECRET = (os.getenv("EDGE_SECRET") or os.getenv("OUTBOX_SECRET") or "").strip()

PUSH_IVL = int(os.getenv("TELEMETRY_INTERVAL_SEC") or 60)
TELEM_DEBUG = str(os.getenv("TELEMETRY_DEBUG") or "").strip().lower() in ("1", "true", "yes", "y", "on")
TELEM_DEBUG_DUMP = str(os.getenv("TELEMETRY_DEBUG_DUMP") or "").strip().lower() in ("1", "true", "yes", "y", "on")

HEARTBEAT_ON_BOOT = str(os.getenv("TELEMETRY_HEARTBEAT_ON_BOOT") or "1").strip().lower() in (
    "1",
    "true",
    "yes",
    "y",
    "on",
)

# Default to daemon unless explicitly set to one-shot.
TELEMETRY_ONESHOT = str(os.getenv("TELEMETRY_ONESHOT") or "").strip().lower() in ("1", "true", "yes", "y", "on")

# Cache file for last payload (helps after deploys / temporary venue auth issues)
CACHE_PATH = os.getenv("TELEMETRY_CACHE_PATH") or "/tmp/nova_edge_telemetry_cache.json"

# Quotes/headline assets (kept from your existing behavior)
QUOTES = {"USD", "USDC", "USDT"}
HEADLINE = ["USDT", "USDC", "USD", "BTC", "ETH"]

# --------------------------------------------------------------------------- #
# Identity helpers
# --------------------------------------------------------------------------- #

def _resolve_agent_id() -> str:
    """
    Canonical agent identity. Never infer from payload or anything external.
    """
    a = (os.getenv("EDGE_AGENT_ID") or os.getenv("AGENT_ID") or "edge").strip()
    return a or "edge"


def _stamp_agent(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Force agent fields to canonical env agent.
    (Prevents stale cached agent IDs like 'edge-primary,edge-nl1' from leaking forward.)
    """
    agent = _resolve_agent_id()
    payload["agent"] = agent
    payload["agent_id"] = agent  # allow Bus to use either
    return payload


# --------------------------------------------------------------------------- #
# HMAC signing
# --------------------------------------------------------------------------- #

def _canonical_json_bytes(obj: Any) -> bytes:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def _sign_body(body_bytes: bytes) -> str:
    if not EDGE_SECRET:
        return ""
    return hmac.new(EDGE_SECRET.encode("utf-8"), body_bytes, hashlib.sha256).hexdigest()


def _post_json(path: str, payload: Dict[str, Any]) -> Tuple[int, str]:
    """
    POST JSON with HMAC signature header.
    Uses X-OUTBOX-SIGN (and also X-NOVA-SIGN for compatibility).
    """
    if not BUS_BASE_URL:
        raise RuntimeError("BUS_BASE_URL/CLOUD_BASE_URL not set on Edge")

    url = BUS_BASE_URL + path
    body = _canonical_json_bytes(payload)
    sig = _sign_body(body)

    headers = {"Content-Type": "application/json"}
    if sig:
        # Canonical header used by Bus verify snippet
        headers["X-OUTBOX-SIGN"] = sig
        # Back-compat if some Bus builds used a different header name
        headers["X-NOVA-SIGN"] = sig

    r = requests.post(url, data=body, headers=headers, timeout=20)
    return r.status_code, (r.text or "")


# --------------------------------------------------------------------------- #
# Cache helpers
# --------------------------------------------------------------------------- #

def _load_cache() -> Optional[Dict[str, Any]]:
    try:
        if not os.path.exists(CACHE_PATH):
            return None
        with open(CACHE_PATH, "r", encoding="utf-8") as f:
            obj = json.load(f)
        if isinstance(obj, dict):
            return obj
    except Exception:
        return None
    return None


def _save_cache(payload: Dict[str, Any]) -> None:
    try:
        tmp = CACHE_PATH + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(payload, f, sort_keys=True)
        os.replace(tmp, CACHE_PATH)
    except Exception:
        pass


# --------------------------------------------------------------------------- #
# Balance collection (preserved style; keep your existing behavior)
# --------------------------------------------------------------------------- #

def _required_venues(by_venue_raw: Dict[str, Any]) -> set[str]:
    """
    Determine which venues we should ensure appear in the payload.
    """
    required = set()
    if isinstance(by_venue_raw, dict):
        for k in by_venue_raw.keys():
            if isinstance(k, str) and k.strip():
                required.add(k.strip().upper())
    # If nothing detected, keep it conservative and do not invent venues.
    return required


def _collect_balances() -> Dict[str, Any]:
    """
    Collect balances from available venue modules.
    This function preserves your existing behavior patterns:
    - attempts to import venue-specific balance readers
    - normalizes to by_venue + flat
    - always stamps canonical agent
    """
    agent = _resolve_agent_id()
    ts = int(time.time())

    by_venue_raw: Dict[str, Any] = {}
    errors: Dict[str, str] = {}

    # --- Coinbase ---
    try:
        # If your repo has Coinbase balance utility, keep using it.
        from coinbase_adv.balance_reader import get_balances as cb_get_balances  # type: ignore
        by_venue_raw["COINBASE"] = cb_get_balances()
    except Exception as e:
        # Non-fatal; record and continue
        errors["COINBASE_FALLBACK"] = f"{type(e).__name__}:{e}"

    # --- BinanceUS ---
    try:
        from binanceus.balance_reader import get_balances as bu_get_balances  # type: ignore
        by_venue_raw["BINANCEUS"] = bu_get_balances()
    except Exception as e:
        errors["BINANCEUS_FALLBACK"] = f"{type(e).__name__}:{e}"

    # --- Kraken ---
    try:
        from kraken.balance_reader import get_balances as kr_get_balances  # type: ignore
        by_venue_raw["KRAKEN"] = kr_get_balances()
    except Exception as e:
        errors["KRAKEN_FALLBACK"] = f"{type(e).__name__}:{e}"

    # Normalize
    by_venue: Dict[str, Dict[str, float]] = {}
    flat: Dict[str, float] = {}

    for venue, vdata in (by_venue_raw or {}).items():
        vkey = (venue or "").upper().strip()
        if not vkey:
            continue

        v_out: Dict[str, float] = {}
        items = None

        # Accept dict-like or list-like inputs
        if isinstance(vdata, dict):
            items = vdata.items()
        elif isinstance(vdata, list):
            # assume list of dicts with {"asset":..., "free":...} etc.
            items = []
            for row in vdata:
                if isinstance(row, dict):
                    a = row.get("asset") or row.get("Asset")
                    x = row.get("free") or row.get("Free") or row.get("amount") or row.get("Amount")
                    if a is not None:
                        items.append((a, x))
        else:
            continue

        if not items:
            continue

        for a, x in items:
            a = (str(a) if a is not None else "").upper().strip()
            if not a:
                continue
            try:
                fx = float(x)
            except Exception:
                continue

            if a in QUOTES:
                v_out[a] = v_out.get(a, 0.0) + fx
            else:
                flat[a] = flat.get(a, 0.0) + fx

        if v_out:
            by_venue[vkey] = v_out

    # Ensure required venues appear (truthful zeros are allowed)
    required = _required_venues(by_venue_raw)
    for v in required:
        if v not in by_venue:
            by_venue[v] = {q: 0.0 for q in QUOTES}
        else:
            for q in QUOTES:
                by_venue[v].setdefault(q, 0.0)

    payload: Dict[str, Any] = {"agent": agent, "by_venue": by_venue, "flat": flat, "ts": ts}
    if errors:
        payload["errors"] = errors

    return _stamp_agent(payload)


# --------------------------------------------------------------------------- #
# Human-readable summary for Edge logs
# --------------------------------------------------------------------------- #

def _summarize_snapshot(payload: dict) -> str:
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
            parts.append(f"{venue}:" + (",".join(bits) if bits else "<no headline quotes>"))

        flat_bits = []
        for asset in HEADLINE:
            if asset in flat:
                try:
                    flat_bits.append(f"{asset}={float(flat[asset]):.4f}")
                except Exception:
                    pass

        err_tail = ""
        if isinstance(errs, dict) and errs:
            err_tail = " errors=" + ",".join(sorted([str(k) for k in errs.keys()]))

        tail = (" || flat:" + ",".join(flat_bits)) if flat_bits else ""
        core = " | ".join(parts) if parts else "no venues"
        return f"agent={agent} {core}{err_tail}{tail}"
    except Exception as e:
        return f"(summary-error: {e})"


# --------------------------------------------------------------------------- #
# Push + loop
# --------------------------------------------------------------------------- #

def push_balances_once(use_cache_if_empty: bool = True) -> bool:
    payload = _collect_balances()

    if use_cache_if_empty and not payload.get("by_venue") and not payload.get("flat"):
        cached = _load_cache()
        if cached:
            payload = cached

    payload = _stamp_agent(payload)

    if TELEM_DEBUG or TELEM_DEBUG_DUMP:
        try:
            summary = _summarize_snapshot(payload)
            print(f"[telemetry] snapshot {summary}")
            if TELEM_DEBUG_DUMP:
                dumped = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
                if len(dumped) > 2000:
                    dumped = dumped[:2000] + "...(truncated)"
                print(f"[telemetry] payload={dumped}")
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
                print(f"[telemetry] push_balances failed {code}: {text[:300]}")
        except Exception as e:
            print(f"[telemetry] error: {e}")
        time.sleep(2**attempt)
    return ok


_STOP = False

def _handle_stop(signum=None, frame=None):
    global _STOP
    _STOP = True


def run_daemon_forever() -> int:
    """
    Long-lived loop. This is what keeps Bus edge_authority fresh.
    """
    global _STOP
    _STOP = False

    signal.signal(signal.SIGTERM, _handle_stop)
    signal.signal(signal.SIGINT, _handle_stop)

    if HEARTBEAT_ON_BOOT:
        push_balances_once(use_cache_if_empty=True)

    while not _STOP:
        start = time.time()
        push_balances_once(use_cache_if_empty=False)
        elapsed = int(time.time() - start)
        sleep_s = max(5, int(PUSH_IVL) - elapsed)
        for _ in range(sleep_s):
            if _STOP:
                break
            time.sleep(1)

    print(f"[telemetry] daemon exiting agent={_resolve_agent_id()}")
    return 0


# --------------------------------------------------------------------------- #
# CLI / module entrypoint
# --------------------------------------------------------------------------- #

def main() -> int:
    if TELEMETRY_ONESHOT:
        ok = push_balances_once(use_cache_if_empty=True)
        print(f"[telemetry] one-shot push ok={ok} agent={_resolve_agent_id()}")
        return 0 if ok else 2

    # Default: daemon loop
    print(
        f"[telemetry] daemon starting agent={_resolve_agent_id()} "
        f"base={(BUS_BASE_URL or '<unset>')} interval={PUSH_IVL}s"
    )
    return run_daemon_forever()


if __name__ == "__main__":
    raise SystemExit(main())
