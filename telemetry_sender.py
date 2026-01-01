import os
import time
import json
import math
import logging
import threading
from typing import Dict, Any, Optional, Tuple

import requests

def get_agent_id(payload: dict | None = None) -> str:
    """Return the canonical agent_id string for this Edge instance.

    Precedence:
      1) payload['agent_id'] / payload['agent'] (if provided)
      2) TELEMETRY_AGENT_ID (explicit override for telemetry only)
      3) AGENT_ID (preferred single identifier)
      4) EDGE_AGENT_ID (legacy alias)
      5) AGENT / AGENT_NAME (legacy)
      6) 'edge'
    """
    if payload:
        v = (payload.get("agent_id") or payload.get("agent") or "").strip()
        if v:
            return v
    for key in ("TELEMETRY_AGENT_ID", "AGENT_ID", "EDGE_AGENT_ID", "AGENT", "AGENT_NAME"):
        v = (os.getenv(key) or "").strip()
        if v:
            return v
    return "edge"


BUS_BASE = (os.getenv("CLOUD_BASE_URL") or os.getenv("BUS_BASE_URL") or os.getenv("BASE_URL") or "").rstrip("/")
TELEMETRY_PATH = os.getenv("BUS_TELEMETRY_PATH") or "/api/telemetry/push"

# Signing header name should match Bus verifier (you’re using X-OUTBOX-SIGN in other endpoints;
# telemetry may use a different one depending on Bus route; keep configurable.)
SIGN_HEADER = os.getenv("TELEMETRY_SIGN_HEADER") or "X-EDGE-SIGN"
SECRET_ENV = os.getenv("TELEMETRY_SECRET_ENV") or "EDGE_SECRET"

TIMEOUT_S = float(os.getenv("TELEMETRY_TIMEOUT_S") or 12)
RETRY_MAX = int(os.getenv("TELEMETRY_RETRY_MAX") or 3)
RETRY_BASE_S = float(os.getenv("TELEMETRY_RETRY_BASE_S") or 1.2)

PUSH_INTERVAL_S = int(os.getenv("BALANCE_PUSH_INTERVAL_S") or 300)
PUSH_ENABLED = str(os.getenv("BALANCE_PUSH_ENABLED") or "1").lower() not in ("0", "false", "no", "off")

LOG_LEVEL = (os.getenv("LOG_LEVEL") or "INFO").upper()
logging.basicConfig(level=getattr(logging, LOG_LEVEL, logging.INFO), format="%(message)s")
log = logging.getLogger("telemetry_sender")


def _hmac_hex(secret: str, body_bytes: bytes) -> str:
    import hmac
    import hashlib
    return hmac.new(secret.encode("utf-8"), body_bytes, hashlib.sha256).hexdigest()


def _now_ts() -> int:
    return int(time.time())


def _safe_float(x: Any) -> float:
    try:
        if x is None:
            return 0.0
        if isinstance(x, (int, float)):
            return float(x)
        s = str(x).strip()
        if not s:
            return 0.0
        return float(s)
    except Exception:
        return 0.0


def _merge_balances_by_venue(raw: Dict[str, Any]) -> Dict[str, Dict[str, float]]:
    """
    Normalize a venue->asset->amount structure.
    Expected input shapes vary across executors; keep tolerant.
    """
    out: Dict[str, Dict[str, float]] = {}
    if not isinstance(raw, dict):
        return out

    for venue, payload in raw.items():
        if venue is None:
            continue
        v = str(venue).strip().upper()
        if not v:
            continue

        out.setdefault(v, {})

        # payload can be dict(asset->amount) or list of dicts, etc.
        if isinstance(payload, dict):
            for asset, amt in payload.items():
                a = str(asset).strip().upper()
                if not a:
                    continue
                out[v][a] = out[v].get(a, 0.0) + _safe_float(amt)

        elif isinstance(payload, list):
            # try list of {"asset": "...", "free": ...} etc.
            for row in payload:
                if not isinstance(row, dict):
                    continue
                asset = row.get("asset") or row.get("Asset") or row.get("currency") or row.get("symbol")
                if not asset:
                    continue
                a = str(asset).strip().upper()
                amt = row.get("free") if "free" in row else row.get("Free") if "Free" in row else row.get("amount") or row.get("Amount") or 0
                out[v][a] = out[v].get(a, 0.0) + _safe_float(amt)

    return out


def _flatten_total(by_venue: Dict[str, Dict[str, float]]) -> Dict[str, float]:
    flat: Dict[str, float] = {}
    for venue, assets in (by_venue or {}).items():
        if not isinstance(assets, dict):
            continue
        for asset, amt in assets.items():
            a = str(asset).strip().upper()
            if not a:
                continue
            flat[a] = flat.get(a, 0.0) + _safe_float(amt)
    return flat


def gather_balances() -> Tuple[Dict[str, Dict[str, float]], Dict[str, float]]:
    """
    Pull balances from the existing Edge venue adapters if present.
    This stays tolerant: if imports fail, we return empty structures.
    """
    raw: Dict[str, Any] = {}

    # BinanceUS / Coinbase / Kraken / MEXC etc can surface via your existing modules.
    # Keep each optional and fail-soft.
    for mod_name, fn_name, venue_key in [
        ("binanceus_wallet_monitor", "get_balances", "BINANCEUS"),
        ("coinbase_wallet_monitor", "get_balances", "COINBASE"),
        ("kraken_wallet_monitor", "get_balances", "KRAKEN"),
        ("mexc_wallet_monitor", "get_balances", "MEXC"),
        ("wallet_monitor", "get_balances", None),  # generic aggregator if you have it
    ]:
        try:
            m = __import__(mod_name, fromlist=[fn_name])
            fn = getattr(m, fn_name, None)
            if not callable(fn):
                continue
            b = fn()
            if venue_key:
                raw[venue_key] = b
            else:
                # If generic returns venue keyed dict, merge it
                if isinstance(b, dict):
                    for k, v in b.items():
                        raw[k] = v
        except Exception:
            continue

    by_venue = _merge_balances_by_venue(raw)
    flat = _flatten_total(by_venue)
    return by_venue, flat


def build_payload() -> Dict[str, Any]:
    agent = get_agent_id()

    by_venue, flat = gather_balances()
    ts = _now_ts()

    payload: Dict[str, Any] = {
        "agent": agent,
        "agent_id": agent,
        "by_venue": by_venue,
        "flat": flat,
        "ts": ts,
    }
    return payload


def _post_json(url: str, payload: Dict[str, Any]) -> Tuple[bool, str]:
    secret = (os.getenv(SECRET_ENV) or "").strip()
    body = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")

    headers = {"Content-Type": "application/json"}
    if secret:
        headers[SIGN_HEADER] = _hmac_hex(secret, body)

    r = requests.post(url, data=body, headers=headers, timeout=TIMEOUT_S)
    if 200 <= r.status_code < 300:
        return True, r.text[:400]
    return False, f"{r.status_code} {r.text[:400]}"


def push_balances(payload: Optional[Dict[str, Any]] = None) -> bool:
    if not BUS_BASE:
        log.warning("[telemetry] BUS_BASE/CLOUD_BASE_URL not set; cannot push telemetry.")
        return False

    payload = payload or build_payload()
    agent = get_agent_id(payload)

    url = BUS_BASE.rstrip("/") + TELEMETRY_PATH
    last_err = ""
    for attempt in range(1, RETRY_MAX + 1):
        try:
            ok, msg = _post_json(url, payload)
            if ok:
                log.info(f"[telemetry] push_balances ok (attempt {attempt}) agent={agent}")
                return True
            last_err = msg
            log.warning(f"[telemetry] push_balances failed (attempt {attempt}) agent={agent} err={msg}")
        except Exception as e:
            last_err = f"{type(e).__name__}: {e}"
            log.warning(f"[telemetry] push_balances exception (attempt {attempt}) agent={agent} err={last_err}")

        time.sleep(RETRY_BASE_S * (attempt ** 2))

    log.warning(f"[telemetry] push_balances final failure agent={agent} err={last_err}")
    return False


# ---- Background pusher (Edge Agent imports this) ----

_pusher_thread: Optional[threading.Thread] = None
_pusher_stop = threading.Event()


def _pusher_loop(interval_s: int) -> None:
    # small start delay to let env + other modules settle
    time.sleep(2.0)
    while not _pusher_stop.is_set():
        try:
            push_balances()
        except Exception as e:
            log.warning(f"[telemetry] pusher_loop exception: {type(e).__name__}: {e}")

        # Sleep in small chunks so stop is responsive
        remaining = max(5, int(interval_s))
        while remaining > 0 and not _pusher_stop.is_set():
            step = min(5, remaining)
            time.sleep(step)
            remaining -= step


def start_balance_pusher(interval_s: Optional[int] = None) -> bool:
    """
    Start a daemon thread that periodically pushes balances telemetry to the Bus.
    This is intentionally import-safe for edge_agent.py.
    """
    global _pusher_thread

    if not PUSH_ENABLED:
        log.info("[telemetry] balance pusher disabled via BALANCE_PUSH_ENABLED=0")
        return False

    if _pusher_thread and _pusher_thread.is_alive():
        return True

    _pusher_stop.clear()
    interval = int(interval_s or PUSH_INTERVAL_S)
    _pusher_thread = threading.Thread(target=_pusher_loop, args=(interval,), daemon=True, name="balance_pusher")
    _pusher_thread.start()
    log.info(f"[telemetry] balance pusher started interval_s={interval} agent={get_agent_id()}")
    return True


def stop_balance_pusher() -> None:
    _pusher_stop.set()


# ---- CLI / one-shot ----

def _fmt_snapshot(by_venue: Dict[str, Dict[str, float]]) -> str:
    parts = []
    for venue in sorted(by_venue.keys()):
        assets = by_venue[venue]
        # show common quotes first if present
        show = []
        for a in ("USD", "USDC", "USDT"):
            if a in assets:
                show.append(f"{a}={assets[a]:.6g}")
        # then a few others if any
        others = [k for k in sorted(assets.keys()) if k not in ("USD", "USDC", "USDT") and abs(assets[k]) > 0]
        for k in others[:6]:
            show.append(f"{k}={assets[k]:.6g}")
        parts.append(f"{venue}:" + ",".join(show))
    return " | ".join(parts)


if __name__ == "__main__":
    # One-shot push for Render web shell testing:
    #   python -m telemetry_sender
    agent = get_agent_id()
    by_venue, flat = gather_balances()

    log.info(f"[telemetry] raw balances venues={len(by_venue)} { _fmt_snapshot(by_venue) }")
    payload = {
        "agent": agent,
        "agent_id": agent,
        "by_venue": by_venue,
        "flat": flat,
        "ts": _now_ts(),
    }
    log.info(f"[telemetry] payload={json.dumps(payload)[:1200]}{'...(truncated)' if len(json.dumps(payload))>1200 else ''}")
    ok = push_balances(payload)
    log.info(f"[telemetry] one-shot push ok={ok} agent={agent}")
