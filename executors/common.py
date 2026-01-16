"""executors.common

Shared helpers used by venue executors.

This module is intentionally tiny and dependency-free.

Bus/Edge compatibility:
Some command producers (Bus) wrap order.place sizing inside intent['payload']:
    {"type":"order.place", "payload": {"amount_usd":10, ...}}
while some Edge executors historically expected sizing fields at the top level.

Use canonicalize_order_place_intent() at the boundary to support both shapes.
"""

from __future__ import annotations

from typing import Any, Dict


def clamp_sell_qty(qty: float, min_qty: float, step: float) -> float:
    """Clamp a sell quantity down to exchange rules.

    - Floors to a multiple of step.
    - Ensures not below min_qty (returns 0.0 if would be invalid).
    """
    try:
        q = float(qty)
        mn = float(min_qty)
        st = float(step)
        if q <= 0 or mn <= 0 or st <= 0:
            return 0.0
        # floor to step
        floored = (q // st) * st
        if floored + 1e-12 < mn:
            return 0.0
        return float(floored)
    except Exception:
        return 0.0


_CANON_PROMOTE_KEYS = (
    "venue",
    "symbol",
    "token",
    "side",
    "mode",
    "note",
    "flags",
    "amount_usd",
    "amount_quote",
    "amount_base",
    "price",
    "price_usd",
    "limit_price",
    "time_in_force",
    "dry_run",
    "idempotency_key",
    "client_order_id",
    "meta",
)


def canonicalize_order_place_intent(intent: Any) -> Dict[str, Any]:
    """Return an Edge-safe order.place intent.

    Behavior (best-effort, never raises):
    - Shallow-copies the dict (does not mutate input)
    - If intent is not dict -> {}
    - Promotes common fields payload->root
    - Normalizes side to uppercase
    - If amount_quote missing and amount_usd provided, sets amount_quote=amount_usd
    - Coerces numeric sizing fields to float when possible
    - Backfills payload with root fields so older consumers still work

    Non-order intents: returned unchanged (shallow copy).
    """
    try:
        if not isinstance(intent, dict):
            return {}

        out: Dict[str, Any] = dict(intent)  # shallow copy
        payload = out.get("payload")
        if not isinstance(payload, dict):
            payload = {}

        # Determine intent type
        itype = (out.get("type") or payload.get("type") or "").strip()

        # If explicitly non-order.place, do nothing
        if itype and itype != "order.place":
            return out

        # Heuristic: treat as order.place only if type matches OR side exists
        if not itype and not (out.get("side") or payload.get("side")):
            return out

        out["type"] = "order.place"

        # Promote missing/empty fields
        for k in _CANON_PROMOTE_KEYS:
            if out.get(k) in (None, "", [], {}):
                v = payload.get(k)
                if v not in (None, ""):
                    out[k] = v

        # Normalize side
        if isinstance(out.get("side"), str):
            out["side"] = out["side"].upper()

        # Default quote sizing
        if out.get("amount_quote") in (None, "", 0, 0.0) and out.get("amount_usd") not in (
            None,
            "",
            0,
            0.0,
        ):
            out["amount_quote"] = out.get("amount_usd")

        # Safe float coercion
        for nk in ("amount_usd", "amount_quote", "amount_base", "price", "price_usd", "limit_price"):
            if nk in out and out[nk] not in (None, ""):
                try:
                    out[nk] = float(out[nk])
                except Exception:
                    pass

        # Backfill payload for older code
        new_payload = dict(payload)
        for k in _CANON_PROMOTE_KEYS:
            if new_payload.get(k) in (None, "", [], {}):
                v = out.get(k)
                if v not in (None, ""):
                    new_payload[k] = v
        out["payload"] = new_payload

        return out
    except Exception:
        return intent if isinstance(intent, dict) else {}
