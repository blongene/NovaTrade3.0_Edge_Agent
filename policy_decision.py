#!/usr/bin/env python3
"""
policy_decision.py

Canonical representation of a policy decision emitted by trade_guard (and,
later, by other policy surfaces).

This is intentionally lightweight and backwards-compatible: callers still
receive a dict, but we centralize the shape and attach a decision_id.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, Optional
import uuid


@dataclass
class PolicyDecision:
    ok: bool
    status: str
    reason: str
    intent: Dict[str, Any]
    patched: Dict[str, Any]

    # Metadata
    decision_id: str = field(default_factory=lambda: uuid.uuid4().hex)
    created_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
    source: str = ""
    venue: str = ""
    symbol: str = ""
    base: str = ""
    quote: str = ""
    requested_amount_usd: Optional[float] = None
    approved_amount_usd: Optional[float] = None

    def to_dict(self) -> Dict[str, Any]:
        """
        Backwards-compatible dict representation that callers of trade_guard
        already expect, but with stable IDs and a small meta block.
        """
        base = {
            "ok": self.ok,
            "status": self.status,
            "reason": self.reason,
            "intent": self.intent,
            "patched": self.patched,
            "decision_id": self.decision_id,
            "created_at": self.created_at,
        }

        meta = {
            "source": self.source,
            "venue": self.venue,
            "symbol": self.symbol,
            "base": self.base,
            "quote": self.quote,
            "requested_amount_usd": self.requested_amount_usd,
            "approved_amount_usd": self.approved_amount_usd,
        }

        # Only include non-empty meta entries
        meta_clean = {
            k: v
            for k, v in meta.items()
            if v is not None and v != ""  # allow 0.0 for amounts
        }
        if meta_clean:
            base["meta"] = meta_clean

        return base
