# telemetry_sender.py — Edge -> Bus balances (push-on-boot + periodic pusher)
#
# Goals:
# - Keep compatibility with older Edge Agent code that imports:
#     from telemetry_sender import start_balance_pusher, _collect_balances
# - Provide a robust one-shot sender (python -m telemetry_sender)
# - Provide a background balance pusher for edge_agent.py
#
# IMPORTANT AGENT ID NOTES
# - The Bus stores telemetry keyed by agent_id.
# - We standardize selection order to avoid "edge-primary" vs "edge-primary,edge-nl1" mismatches.
#
# Priority:
#   1) EDGE_AGENT_ID  (preferred; may be compound like 'edge-primary,edge-nl1')
#   2) AGENT_ID
#   3) AGENT (legacy)
#   4) "edge"
#
# Environment (typical):
# - CLOUD_BASE_URL or BUS_BASE_URL: https://novatrade3-0.onrender.com
# - TELEMETRY_PUSH_PATH (optional): /api/telemetry/push
# - TELEMETRY_TIMEOUT_S (optional): request timeout
#
# This module intentionally avoids heavy dependencies and is safe to import from edge_agent.py.

from __future__ import annotations

import json
import os
import time
import threading
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

import requests


TRUE_STRINGS = {
    "1",
    "true",
    "t",
    "y",
    "yes",
    "on",
}

# Which assets to highlight in the "flat" section. These are the ones most
# likely to matter day-to-day (USD, USDC, USDT, BTC, ETH, etc.). The rest of the
# assets are still included in the flat dict, but many will be 0.
DEFAULT_FLAT_ASSETS = [
    "USD",
    "USDC",
    "USDT",
    "BTC",
    "ETH",
]

DEFAULT_TIMEOUT_S = int(os.getenv("TELEMETRY_TIMEOUT_S") or 12)
DEFAULT_PUSH_INTERVAL_S = int(os.getenv("TELEMETRY_PUSH_INTERVAL_S") or 300)

DEFAULT_PUSH_PATH = os.getenv("TELEMETRY_PUSH_PATH") or "/api/telemetry/push"


def _now_ts() -> int:
    return int(time.time())


def _bool_env(name: str, default: bool = False) -> bool:
    v = (os.getenv(name) or "").strip().lower()
    if not v:
        return default
    return v in TRUE_STRINGS


def _resolve_bus_base_url() -> str:
    """
    Resolve the Bus base URL for telemetry push.
    Priority:
      - BUS_BASE_URL
      - CLOUD_BASE_URL
      - BASE_URL
      - PUBLIC_BASE_URL
    """
    base = (
        os.getenv("BUS_BASE_URL")
        or os.getenv("CLOUD_BASE_URL")
        or os.getenv("BASE_URL")
        or os.getenv("PUBLIC_BASE_URL")
        or ""
    ).strip()
    return base.rstrip("/")


def _resolve_agent_id() -> str:
    """
    Resolve the agent identifier.
    Priority:
      1) EDGE_AGENT_ID
      2) AGENT_ID
      3) AGENT (legacy)
      4) "edge"
    """
    agent = (os.getenv("EDGE_AGENT_ID") or os.getenv("AGENT_ID") or os.getenv("AGENT") or "edge").strip()
    return agent or "edge"


def _compact_float(x: Any) -> float:
    try:
        if x is None:
            return 0.0
        return float(x)
    except Exception:
        return 0.0


def _safe_print(prefix: str, msg: str) -> None:
    # Keep logs human-readable in Render shells
    try:
        print(prefix + " " + msg)
    except Exception:
        pass


def _try_import_coinbase_jwt_generator() -> None:
    """
    Optional: for Coinbase Advanced integrations, some environments require
    importing jwt_generator to initialize. We tolerate failures.
    """
    try:
        from coinbase import jwt_generator  # noqa: F401
        _safe_print("[coinbase_adv]", "jwt_generator OK via import 'coinbase.jwt_generator'")
    except Exception as e:
        _safe_print("[coinbase_adv]", f"jwt_generator import skipped ({type(e).__name__}: {e})")


@dataclass
class VenueBalances:
    usd: float = 0.0
    usdc: float = 0.0
    usdt: float = 0.0
    # plus any other assets as dynamic dict


def _normalize_asset(sym: str) -> str:
    return (sym or "").strip().upper()


def _merge_asset_dict(dst: Dict[str, float], src: Dict[str, Any]) -> None:
    for k, v in (src or {}).items():
        kk = _normalize_asset(k)
        dst[kk] = dst.get(kk, 0.0) + _compact_float(v)


def _collect_from_coinbase() -> Dict[str, float]:
    """
    Try to obtain balances from coinbase executor (if present).
    Returns dict of asset -> free amount (float).
    """
    try:
        # Many variants exist; we try common patterns safely.
        from coinbase_executor import get_balances as cb_get_balances  # type: ignore
        out = cb_get_balances()
        if isinstance(out, dict):
            # expected { "USD": 1.23, "USDC": 4.56, ... }
            return { _normalize_asset(k): _compact_float(v) for k, v in out.items() }
    except Exception:
        pass

    try:
        from coinbase_executor import fetch_balances as cb_fetch_balances  # type: ignore
        out = cb_fetch_balances()
        if isinstance(out, dict):
            return { _normalize_asset(k): _compact_float(v) for k, v in out.items() }
    except Exception:
        pass

    return {}


def _collect_from_binanceus() -> Dict[str, float]:
    try:
        from binanceus_executor import get_balances as bx_get_balances  # type: ignore
        out = bx_get_balances()
        if isinstance(out, dict):
            return { _normalize_asset(k): _compact_float(v) for k, v in out.items() }
    except Exception:
        pass

    try:
        from binanceus_executor import fetch_balances as bx_fetch_balances  # type: ignore
        out = bx_fetch_balances()
        if isinstance(out, dict):
            return { _normalize_asset(k): _compact_float(v) for k, v in out.items() }
    except Exception:
        pass

    return {}


def _collect_from_kraken() -> Dict[str, float]:
    try:
        from kraken_executor import get_balances as kk_get_balances  # type: ignore
        out = kk_get_balances()
        if isinstance(out, dict):
            return { _normalize_asset(k): _compact_float(v) for k, v in out.items() }
    except Exception:
        pass

    try:
        from kraken_executor import fetch_balances as kk_fetch_balances  # type: ignore
        out = kk_fetch_balances()
        if isinstance(out, dict):
            return { _normalize_asset(k): _compact_float(v) for k, v in out.items() }
    except Exception:
        pass

    return {}


def _collect_from_mexc() -> Dict[str, float]:
    try:
        from mexc_executor import get_balances as mx_get_balances  # type: ignore
        out = mx_get_balances()
        if isinstance(out, dict):
            return { _normalize_asset(k): _compact_float(v) for k, v in out.items() }
    except Exception:
        pass

    try:
        from mexc_executor import fetch_balances as mx_fetch_balances  # type: ignore
        out = mx_fetch_balances()
        if isinstance(out, dict):
            return { _normalize_asset(k): _compact_float(v) for k, v in out.items() }
    except Exception:
        pass

    return {}


def _collect_balances() -> Tuple[Dict[str, Dict[str, float]], Dict[str, float]]:
    """
    Collect balances from enabled venues, returning:
      by_venue: { "COINBASE": {"USD": 1.0, "USDC": 2.0, ...}, ... }
      flat:     { "USD": 1.0, "USDC": 2.0, ... } aggregated across venues

    This is intentionally tolerant: missing executors simply return empty dicts.
    """
    by_venue: Dict[str, Dict[str, float]] = {}
    flat: Dict[str, float] = {}

    # If your system uses ROUTER_ALLOWED or ENABLE_* flags, we can honor them
    # lightly, but default to "try and tolerate".
    router_allowed = (os.getenv("ROUTER_ALLOWED") or "").strip().upper()
    allow = set([x.strip() for x in router_allowed.split(",") if x.strip()]) if router_allowed else set()

    def allowed(name: str) -> bool:
        if not allow:
            return True
        return name.upper() in allow

    # Coinbase
    if allowed("COINBASE"):
        cb = _collect_from_coinbase()
        if cb:
            by_venue["COINBASE"] = cb
            _merge_asset_dict(flat, cb)

    # BinanceUS
    if allowed("BINANCEUS"):
        bx = _collect_from_binanceus()
        if bx:
            by_venue["BINANCEUS"] = bx
            _merge_asset_dict(flat, bx)

    # Kraken
    if allowed("KRAKEN"):
        kk = _collect_from_kraken()
        if kk:
            by_venue["KRAKEN"] = kk
            _merge_asset_dict(flat, kk)

    # MEXC (optional)
    if allowed("MEXC"):
        mx = _collect_from_mexc()
        if mx:
            by_venue["MEXC"] = mx
            _merge_asset_dict(flat, mx)

    # Ensure flat has at least the default core assets for stable output
    for a in DEFAULT_FLAT_ASSETS:
        flat.setdefault(a, 0.0)

    return by_venue, flat


def _format_raw_balances(by_venue: Dict[str, Dict[str, float]]) -> str:
    """
    Pretty short line: "COINBASE:USD=...,USDC=... | BINANCEUS:..."
    """
    parts = []
    for venue, assets in by_venue.items():
        if not assets:
            continue
        # prioritize USD/USDC/USDT first
        keys = ["USD", "USDC", "USDT"] + [k for k in sorted(assets.keys()) if k not in ("USD", "USDC", "USDT")]
        kv = []
        for k in keys:
            if k in assets:
                kv.append(f"{k}={assets[k]:.6g}")
        parts.append(f"{venue}:" + ",".join(kv[:10]))
    return " | ".join(parts)


def _build_snapshot(agent: str, by_venue: Dict[str, Dict[str, float]], flat: Dict[str, float]) -> str:
    """
    Human snapshot string similar to what you’ve been seeing in Render logs.
    """
    chunks = []
    for venue, assets in by_venue.items():
        usd = assets.get("USD", 0.0)
        usdc = assets.get("USDC", 0.0)
        usdt = assets.get("USDT", 0.0)
        chunks.append(f"{venue}:USDT={usdt:.2f},USDC={usdc:.2f},USD={usd:.2f}")
    btc = flat.get("BTC", 0.0)
    eth = flat.get("ETH", 0.0)
    return f"agent={agent} " + " | ".join(chunks) + f" || flat:BTC={btc:.4g},ETH={eth:.4g}"


def _post_json(url: str, payload: Dict[str, Any], headers: Optional[Dict[str, str]] = None) -> Tuple[bool, str]:
    try:
        r = requests.post(url, json=payload, headers=headers or {}, timeout=DEFAULT_TIMEOUT_S)
        if r.status_code >= 200 and r.status_code < 300:
            return True, r.text[:200]
        return False, f"{r.status_code} {r.text[:200]}"
    except Exception as e:
        return False, f"{type(e).__name__}: {e}"


def push_balances_once() -> bool:
    """
    One-shot telemetry push.
    """
    _try_import_coinbase_jwt_generator()

    base = _resolve_bus_base_url()
    agent = _resolve_agent_id()
    ts = _now_ts()

    by_venue, flat = _collect_balances()

    _safe_print("[telemetry]", f"raw balances venues={len(by_venue)} {_format_raw_balances(by_venue)}")
    _safe_print("[telemetry]", f"snapshot {_build_snapshot(agent, by_venue, flat)}")

    payload: Dict[str, Any] = {
        "agent": agent,
        "agent_id": agent,
        "by_venue": by_venue,
        "flat": flat,
        "ts": ts,
    }

    if not base:
        _safe_print("[telemetry]", "No BUS_BASE_URL/CLOUD_BASE_URL set; skipping push (dry run only).")
        _safe_print("[telemetry]", "payload=" + json.dumps(payload)[:500] + ("...(truncated)" if len(json.dumps(payload)) > 500 else ""))
        return False

    url = base + DEFAULT_PUSH_PATH
    _safe_print("[telemetry]", "payload=" + json.dumps(payload)[:500] + ("...(truncated)" if len(json.dumps(payload)) > 500 else ""))

    ok, msg = _post_json(url, payload)
    if ok:
        _safe_print("[telemetry]", "push_balances ok (attempt 1)")
        return True
    _safe_print("[telemetry]", f"push_balances FAILED: {msg}")
    return False


def start_balance_pusher(
    interval_s: int = DEFAULT_PUSH_INTERVAL_S,
    initial_delay_s: int = 0,
    stop_event: Optional[threading.Event] = None,
) -> threading.Thread:
    """
    Background thread that pushes balances every interval_s seconds.

    edge_agent.py imports and uses this function. Keep signature tolerant.
    """
    if stop_event is None:
        stop_event = threading.Event()

    def _loop() -> None:
        if initial_delay_s and initial_delay_s > 0:
            time.sleep(initial_delay_s)

        while not stop_event.is_set():
            try:
                push_balances_once()
            except Exception as e:
                _safe_print("[telemetry]", f"balance pusher exception: {type(e).__name__}: {e}")

            # wait with interruptibility
            end = time.time() + max(5, int(interval_s))
            while time.time() < end:
                if stop_event.is_set():
                    break
                time.sleep(1)

    t = threading.Thread(target=_loop, name="balance_pusher", daemon=True)
    t.start()
    return t


def main() -> None:
    ok = push_balances_once()
    _safe_print("[telemetry]", f"one-shot push ok={ok} agent={_resolve_agent_id()}")


if __name__ == "__main__":
    main()
