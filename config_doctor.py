"""config_doctor.py — lightweight config validation (Phase 29 safe)

This does NOT change behavior. It only reports configuration risks that can
increase surprise, noise, or blast radius.

Design goals:
  - Never raise
  - One concise summary line (stdout)
  - Small set of high-signal checks
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import List, Tuple


def _env(key: str, default: str = "") -> str:
    return (os.getenv(key) or default).strip()


@dataclass
class DoctorResult:
    ok: bool
    warnings: List[str]
    hints: List[str]


def _same(a: str, b: str) -> bool:
    return bool(a) and bool(b) and a == b


def diagnose() -> DoctorResult:
    warnings: List[str] = []
    hints: List[str] = []

    edge_mode = _env("EDGE_MODE", "dry").lower()
    live_armed = _env("LIVE_ARMED", "").upper()

    # 1) Live arming guard (warn only)
    if edge_mode == "live" and live_armed != "YES":
        warnings.append("EDGE_MODE=live but LIVE_ARMED!=YES (will be hard-blocked)")
        hints.append("Set LIVE_ARMED=YES only when intentionally going live")

    # 2) Secret separation
    edge_secret = _env("EDGE_SECRET")
    telemetry_secret = _env("TELEMETRY_SECRET")
    outbox_secret = _env("OUTBOX_SECRET")
    if _same(edge_secret, telemetry_secret):
        warnings.append("EDGE_SECRET equals TELEMETRY_SECRET (blast radius)")
        hints.append("Use distinct secrets for command auth vs telemetry")
    if _same(edge_secret, outbox_secret) and outbox_secret:
        warnings.append("EDGE_SECRET equals OUTBOX_SECRET (blast radius)")
    if _same(telemetry_secret, outbox_secret) and outbox_secret:
        warnings.append("TELEMETRY_SECRET equals OUTBOX_SECRET (blast radius)")

    # 3) Duplicate Binance vars (BinanceUS vs BINANCE)
    binanceus_key = _env("BINANCEUS_API_KEY")
    binance_key = _env("BINANCE_API_KEY")
    if binanceus_key and binance_key:
        warnings.append("Both BINANCEUS_* and BINANCE_* are set (routing ambiguity)")
        hints.append("Prefer BINANCEUS_* only; unset BINANCE_* to avoid surprises")

    # 4) Venue allowlist sanity
    allowed = _env("ROUTER_ALLOWED", "").upper()
    if allowed:
        parts = [p.strip() for p in allowed.split(",") if p.strip()]
        known = {"COINBASE", "BINANCEUS", "KRAKEN", "MEXC"}
        unknown = [p for p in parts if p not in known]
        if unknown:
            warnings.append(f"ROUTER_ALLOWED contains unknown venues: {','.join(unknown)}")
            hints.append("Check spelling; keep allowlist tight during observation")

    # 5) Telemetry debug dump noise
    if _env("TELEMETRY_DEBUG_DUMP", "0") in {"1", "true", "yes"}:
        warnings.append("TELEMETRY_DEBUG_DUMP enabled (log noise)")
        hints.append("Set TELEMETRY_DEBUG_DUMP=0 for Phase 29")

    ok = len(warnings) == 0
    return DoctorResult(ok=ok, warnings=warnings, hints=hints)


def emit_once(prefix: str = "CONFIG") -> DoctorResult:
    """Run diagnose() and print a single concise line.

    Example:
      [CONFIG] PASS
      [CONFIG] WARN 2 — msg1 | msg2
    """
    try:
        r = diagnose()
        if r.ok:
            print(f"[{prefix}] PASS")
        else:
            joined = " | ".join(r.warnings[:6])
            more = "" if len(r.warnings) <= 6 else f" (+{len(r.warnings)-6} more)"
            print(f"[{prefix}] WARN {len(r.warnings)} — {joined}{more}")
        return r
    except Exception:
        # Never block startup because of the doctor
        print(f"[{prefix}] WARN 1 — config_doctor_failed")
        return DoctorResult(ok=False, warnings=["config_doctor_failed"], hints=[])
