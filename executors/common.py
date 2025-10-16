def clamp_sell_qty(free_base: float, req_base: float, step: float, reserve: float = 0.0) -> float:
    # never exceed free balance minus small reserve; snap down to step size
    qty = max(0.0, min(req_base, max(0.0, free_base - reserve)))
    if step and step > 0:
        # floor to lot size to avoid "min lot/precision" errors
        qty = (qty // step) * step
    return qty
