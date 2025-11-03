
from __future__ import annotations
# pollers/binanceus_poll.py
import time
from typing import Optional, Dict, Any

def poll_binanceus(client_id: str, venue_symbol: str, wait_sec: int = 2, max_retry: int = 3) -> Optional[Dict[str, Any]]:
    try:
        from executors.binance_us_executor import BinanceUS
    except Exception:
        return None
    try:
        api = BinanceUS()
    except Exception:
        return None

    symbol = venue_symbol.replace('-', '')
    tries = 0
    while tries <= max_retry:
        if tries > 0:
            time.sleep(wait_sec)
        tries += 1
        data = None
        try:
            data = api.get_order(symbol=symbol, origClientOrderId=client_id)  # type: ignore
        except TypeError:
            try:
                data = api.get_order(symbol=symbol, clientOrderId=client_id)  # type: ignore
            except Exception:
                data = None
        except Exception:
            data = None
        if not data:
            continue

        fills = []
        total_qty = 0.0
        notional  = 0.0
        try:
            cumm = float(data.get('cummulativeQuoteQty') or 0.0)
            qty  = float(data.get('executedQty') or 0.0)
            price= float(data.get('avgPrice') or 0.0)
            if qty and cumm and (price or (qty and cumm)):
                if not price and qty:
                    price = cumm/qty
                return {'fills': fills, 'executed_qty': qty, 'avg_price': round(price,12), 'raw': data}
        except Exception:
            pass
        for f in (data.get('fills') or []):
            try:
                px = float(f.get('price') or 0.0)
                q  = float(f.get('qty') or 0.0)
                if q and px:
                    fills.append({'qty': q, 'price': px})
                    total_qty += q
                    notional  += q*px
            except Exception:
                continue
        if total_qty > 0 and notional > 0:
            return {'fills': fills, 'executed_qty': total_qty, 'avg_price': round(notional/total_qty, 12), 'raw': data}
    return None
