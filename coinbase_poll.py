
from __future__ import annotations
# pollers/coinbase_poll.py
import time
from typing import Optional, Dict, Any

def _first(*funcs):
    for f in funcs:
        if callable(f):
            return f
    return None

def poll_coinbase(client_id: str, venue_symbol: str, wait_sec: int = 2, max_retry: int = 3) -> Optional[Dict[str, Any]]:
    try:
        from executors.coinbase_advanced_executor import CoinbaseCDP
    except Exception:
        return None
    try:
        cdp = CoinbaseCDP()
    except Exception:
        return None

    getter = _first(getattr(cdp, 'get_order', None), getattr(cdp, 'order', None), getattr(cdp, 'fetch_order', None))
    if getter is None:
        return None

    tries = 0
    while tries <= max_retry:
        if tries > 0:
            time.sleep(wait_sec)
        tries += 1
        order = None
        try:
            order = getter(client_id=client_id)  # type: ignore
        except TypeError:
            try:
                order = getter(client_order_id=client_id)  # type: ignore
            except Exception:
                order = None
        except Exception:
            order = None
        if not order:
            continue

        fills = []
        total_qty = 0.0
        notional = 0.0
        legs = []
        for key in ('fills','leg_fills','executions'):
            v = order.get(key) if isinstance(order, dict) else getattr(order, key, None)
            if v:
                legs = v; break
        for f in legs or []:
            try:
                qty = float(f.get('size') or f.get('qty') or f.get('quantity') or 0.0)
                px  = float(f.get('price') or f.get('avg_price') or 0.0)
                if qty and px:
                    fills.append({'qty': qty, 'price': px})
                    total_qty += qty
                    notional  += qty*px
            except Exception:
                continue
        if total_qty > 0 and notional > 0:
            return {'fills': fills, 'executed_qty': total_qty, 'avg_price': round(notional/total_qty, 12), 'raw': order}
    return None
