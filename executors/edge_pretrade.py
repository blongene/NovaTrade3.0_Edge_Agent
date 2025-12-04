#!/usr/bin/env python3
"""
edge_pretrade.py — venue rule + pre-flight validator for Edge Agent.

This is used by binance_us_executor (and later other executors) via:

    from edge_pretrade import pretrade_validate

It MUST NEVER raise. On any error it should fall back to "ok" so that
the executor behaviour is at least as safe as before.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Tuple, Optional

import logging
import math

log = logging.getLogger("edge_pretrade")


# ---------------------------------------------------------------------------
# Venue rule model
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class VenueRule:
    """
    Simple rule set for a venue/symbol.

    All values are in *quote* terms unless noted otherwise.
    - min_notional: minimum notional value (e.g. 10 USDT) required by venue
    - min_qty:      minimum BASE quantity (e.g. 0.00001 BTC)
    """
    min_notional: float = 0.0
    min_qty: float = 0.0


# Rules are intentionally conservative. If we don't know a rule, we prefer to
# allow the trade and let the venue complain rather than block a valid trade.
VENUE_RULES: Dict[str, Dict[str, VenueRule]] = {
    "BINANCEUS": {
        # Symbol-specific rules
        "BTCUSDT": VenueRule(min_notional=10.0, min_qty=1e-6),
        "BTCUSD":  VenueRule(min_notional=10.0, min_qty=1e-6),

        # You can tune these as we learn more symbols
        "OCEANUSDT": VenueRule(min_notional=5.0),

        # Quote-level defaults (any pair quoted in these stables)
        "USD":  VenueRule(min_notional=10.0),
        "USDT": VenueRule(min_notional=10.0),
        "USDC": VenueRule(min_notional=10.0),

        # Generic fallback for the venue
        "_DEFAULT": VenueRule(min_notional=10.0),
    },

    # Placeholders so we can safely extend to other venues later
    "COINBASE": {
        "_DEFAULT": VenueRule(min_notional=1.0),
    },
    "KRAKEN": {
        "_DEFAULT": VenueRule(min_notional=5.0),
    },
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return default


def _compact_symbol(base: str, quote: str, symbol: Optional[str] = None) -> str:
    """
    Produce a compact symbol string like BTCUSDT regardless of whether
    we get "BTC/USDT", "BTC-USDT" or "BTCUSDT".
    """
    if symbol:
        s = symbol.replace("/", "").replace("-", "").upper()
        if s:
            return s
    return f"{(base or '').upper()}{(quote or '').upper()}"


def _lookup_rule(venue: str, base: str, quote: str,
                 symbol: Optional[str] = None) -> Optional[VenueRule]:
    v = (venue or "").upper()
    b = (base or "").upper()
    q = (quote or "").upper()
    sym = _compact_symbol(b, q, symbol)

    venue_rules = VENUE_RULES.get(v)
    if not venue_rules:
        return None

    # Priority: exact symbol > quote-level rule > venue default
    return (
        venue_rules.get(sym)
        or venue_rules.get(q)
        or venue_rules.get("_DEFAULT")
    )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def pretrade_validate(
    *,
    venue: str,
    base: str,
    quote: str,
    price: float,
    amount_base: float,
    amount_quote: float,
    venue_balances: Optional[Dict[str, Any]] = None,
) -> Tuple[bool, str, str, float, float]:
    """
    Main entrypoint used by binance_us_executor.

    Returns:
        ok:          True if trade is allowed, False if vetoed
        reason:      Human-readable reason (shown in Trade_Log / Policy_Log)
        chosen_quote: Usually the quote symbol (unchanged for now)
        min_size:    For now: the min_notional enforced (0.0 if none)
        max_size:    Reserved for future (always 0.0 right now)

    It must NEVER raise.
    """
    try:
        v = (venue or "").upper()
        b = (base or "").upper()
        q = (quote or "").upper()

        amt_base = _safe_float(amount_base)
        amt_quote = _safe_float(amount_quote)

        # If we somehow get nonsense sizing, just allow and let the venue error;
        # that’s safer than blowing up the executor.
        if amt_base <= 0.0 or amt_quote <= 0.0:
            return True, "ok", q, 0.0, 0.0

        rule = _lookup_rule(v, b, q)
        if not rule:
            # No rules known for this venue — allow
            return True, "ok", q, 0.0, 0.0

        notional = amt_quote
        min_notional = float(rule.min_notional or 0.0)
        min_qty = float(rule.min_qty or 0.0)

        # --- Venue min-notional check ---------------------------------------
        if min_notional > 0.0 and notional + 1e-9 < min_notional:
            reason = (
                f"below {v} min_notional {min_notional:g} {q} "
                f"(got {notional:g})"
            )
            log.info(
                "pretrade veto: %s %s/%s %s",
                v,
                b,
                q,
                reason,
            )
            return False, reason, q, min_notional, 0.0

        # --- Venue min-qty check (if defined) -------------------------------
        if min_qty > 0.0 and amt_base + 1e-12 < min_qty:
            reason = (
                f"below {v} min_qty {min_qty:g} {b} (got {amt_base:g})"
            )
            log.info(
                "pretrade veto: %s %s/%s %s",
                v,
                b,
                q,
                reason,
            )
            return False, reason, q, min_qty, 0.0

        # Future: we can also add "don’t spend more than X% of balance"
        # using venue_balances, but we’ll keep that out of scope for now.

        return True, "ok", q, min_notional or min_qty or 0.0, 0.0

    except Exception as e:  # pragma: no cover - last-ditch safety
        # Absolutely must not break the executor: log and allow.
        log.exception("pretrade_validate failed; allowing trade: %s", e)
        return True, "ok", quote, 0.0, 0.0
