
from __future__ import annotations
# pollers/kraken_poll.py
import time
from typing import Optional, Dict, Any

def poll_kraken(client_id: str, venue_symbol: str, wait_sec: int = 2, max_retry: int = 3) -> Optional[Dict[str, Any]]:
    try:
        from executors.kraken_executor import kraken_client
    except Exception:
        kraken_client = None
    api = None
    try:
        api = kraken_client() if callable(kraken_client) else None
    except Exception:
        api = None
    if api is None:
        return None

    tries = 0
    while tries <= max_retry:
        if tries > 0:
            time.sleep(wait_sec)
        tries += 1

        data = None
        for name in ('query_orders','get_order','fetch_order','order'):
            fn = getattr(api, name, None)
            if not callable(fn):
                continue
            try:
                data = fn(userref=client_id)  # type: ignore
            except TypeError:
                try:
                    data = fn(client_id=client_id)  # type: ignore
                except Exception:
                    data = None
            except Exception:
                data = None
            if data:
                break
        if not data:
            continue

        fills = []
        total_qty = 0.0
        notional  = 0.0
        order = None
        try:
            res = data.get('result') or {}
            if isinstance(res, dict) and res:
                order = next(iter(res.values()))
        except Exception:
            order = data if isinstance(data, dict) else None
        if not order:
            continue

        try:
            qty = float(order.get('vol_exec') or 0.0)
            price = float(order.get('price') or order.get('avg_price') or 0.0)
            if qty and price:
                return {'fills': [], 'executed_qty': qty, 'avg_price': round(price,12), 'raw': order}
        except Exception:
            pass

        for f in (order.get('trades') or []):
            try:
                px = float(f.get('price') or 0.0)
                q  = float(f.get('qty') or f.get('vol') or 0.0)
                if px and q:
                    fills.append({'qty': q, 'price': px})
                    total_qty += q
                    notional  += q*px
            except Exception:
                continue
        if total_qty > 0 and notional > 0:
            return {'fills': fills, 'executed_qty': total_qty, 'avg_price': round(notional/total_qty,12), 'raw': order}
    return None
