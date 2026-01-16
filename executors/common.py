def clamp_sell_qty(free_base: float, req_base: float, step: float, reserve: float = 0.0) -> float:
    # never exceed free balance minus small reserve; snap down to step size
    qty = max(0.0, min(req_base, max(0.0, free_base - reserve)))
    if step and step > 0:
        # floor to lot size to avoid "min lot/precision" errors
        qty = (qty // step) * step
    return qty

def canonicalize_order_place_intent(intent: dict) -> dict:
    """
    Ensure order.place intents have sizing fields at top-level.
    Promotes payload -> root and normalizes side.
    """
    if not isinstance(intent, dict):
        return {}

    out = dict(intent)
    payload = out.get("payload")
    if not isinstance(payload, dict):
        payload = {}

    itype = (out.get("type") or payload.get("type") or "").strip()

    if itype and itype != "order.place":
        return out

    if not itype and not (out.get("side") or payload.get("side")):
        return out

    out["type"] = "order.place"

    for k in (
        "venue","symbol","token","side","mode","note","flags","meta",
        "amount_usd","amount_quote","amount_base",
        "dry_run","idempotency_key","client_order_id",
        "price","limit_price","time_in_force",
    ):
        if out.get(k) in (None, "", [], {}):
            v = payload.get(k)
            if v not in (None, ""):
                out[k] = v

    if isinstance(out.get("side"), str):
        out["side"] = out["side"].upper()

    if out.get("amount_quote") in (None, "", 0, 0.0) and out.get("amount_usd") not in (None, "", 0, 0.0):
        out["amount_quote"] = out.get("amount_usd")

    for nk in ("amount_usd","amount_quote","amount_base","price","limit_price"):
        if nk in out and out[nk] not in (None, ""):
            try:
                out[nk] = float(out[nk])
            except Exception:
                pass

    # mirror root -> payload for safety
    new_payload = dict(payload)
    for k in (
        "venue","symbol","token","side","mode","note","flags","meta",
        "amount_usd","amount_quote","amount_base",
        "dry_run","idempotency_key","client_order_id",
        "price","limit_price","time_in_force",
    ):
        if new_payload.get(k) in (None, "", [], {}):
            v = out.get(k)
            if v not in (None, ""):
                new_payload[k] = v
    out["payload"] = new_payload

    return out
