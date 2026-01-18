"""safety_doctrine.py — hard safety gates for live execution.

This file encodes the Phase 29+ doctrine:
  - Capital preservation outranks alpha
  - Psychological safety outranks curiosity

Hard rule implemented here:
  Live execution is only allowed when explicitly armed.

Why:
  Prevent accidental live trading due to env drift or command shape changes.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Optional


def _env(key: str, default: str = "") -> str:
    return (os.getenv(key) or default).strip()


@dataclass
class LiveGate:
    allowed: bool
    reason: str


def live_gate(intent_mode: Optional[str] = None) -> LiveGate:
    """Return whether live execution is allowed.

    Conditions:
      - EDGE_MODE must be 'live'
      - Effective mode (intent_mode or EDGE_MODE) must be 'live'
      - EDGE_HOLD must be false
      - LIVE_ARMED must be YES (explicit operator arming)

    NOTE: This gate is intentionally minimal and deterministic.
    """

    edge_mode = _env("EDGE_MODE", "dry").lower()
    hold = _env("EDGE_HOLD", "false").lower() in {"1", "true", "yes"}
    effective = (intent_mode or edge_mode or "").lower()
    live_armed = _env("LIVE_ARMED", "").upper()

    if hold:
        return LiveGate(False, "edge_hold=true")
    if edge_mode != "live":
        return LiveGate(False, f"edge_mode={edge_mode}")
    if effective != "live":
        return LiveGate(False, f"intent_mode={effective or 'unset'}")
    if live_armed != "YES":
        return LiveGate(False, "live_not_armed")
    return LiveGate(True, "live_allowed")
