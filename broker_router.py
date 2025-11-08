# broker_router.py — venue router: Coinbase Advanced, Binance.US, Kraken, MEXC
import time
from typing import Dict, Any

def _as_ok(receipt: Dict[str, Any]) -> Dict[str, Any]:
    # normalize into a common result shape for edge_agent
    if not isinstance(receipt, dict):
        return {"status": "error", "message": "executor returned non-dict", "fills": []}
    ok = receipt.get("ok")
    if ok is True or receipt.get("status") in {"ok", "filled"}:
        return {
            "status": "ok",
            "txid": receipt.get("txid") or receipt.get("response", {}).get("order_id"),
            "fills": receipt.get("fills", []),
            "message": receipt.get("message", "ok"),
        }
    return {"status": "error", "message": receipt.get("error") or receipt.get("message") or "unknown", "fills": []}

def execute(cmd: Dict[str, Any]) -> Dict[str, Any]:
    payload = (cmd or {}).get("payload", {})
    venue   = (payload.get("venue") or "").upper()
    side    = (payload.get("side") or "BUY").upper()
    symbol  = payload.get("symbol", "BTC/USDT")
    quote   = float(payload.get("amount") or payload.get("quote_amount") or 0.0)
    if quote <= 0:
        return {"status": "error", "message": "amount/quote_amount must be > 0", "fills": []}

    # ---- Coinbase Advanced ----
    if venue in {"COINBASE", "COINBASE_ADV", "COINBASE ADVANCED"}:
        from coinbase_advanced_executor import execute as cb_exec
        return _as_ok(cb_exec({"payload": {"side": side, "symbol": symbol, "quote_amount": quote}}))

    # ---- Binance.US ----
    if venue in {"BINANCEUS", "BINANCE.US", "BINANCE_US"}:
        from binanceus_executor import place_market_order
        client_order_id = payload.get("client_order_id") or f"EDGE-{int(time.time()*1000)}"
        rec = place_market_order(
            client_order_id=client_order_id,
            symbol=symbol,
            side=side,
            amount=str(quote)  # Binance.US expects strings for qty/quoteOrderQty
        )
        return _as_ok(rec)

    # ---- Kraken (optional) ----
    if venue == "KRAKEN":
        from kraken_executor import execute as kraken_exec
        return _as_ok(kraken_exec(cmd))

    return {"status": "error", "message": f"unknown or disabled venue: {venue}", "fills": []}

# broker_router.py
def route(order: dict):
    """
    Returns (ok: bool, receipt: dict).
    Receipt is safe to embed under the 'receipt' key for /api/commands/ack.
    """
    venue = (order.get("venue") or "").upper()
    symbol = (order.get("symbol") or "").upper()
    side   = (order.get("side") or "").lower()
    amount = float(order.get("amount") or 0)

    if amount <= 0:
        return False, {"error": "amount/quote_amount must be > 0", "applied": order}

    # ... dispatch to venue executor, get `r` back (may have local 'status' strings) ...
    ok = (r is True) or (isinstance(r, dict) and (r.get("ok") is True or r.get("status") in {"ok", "filled", "✅ Executed"}))

    normalized = {
        "normalized": {
            "venue": venue,
            "symbol": symbol,
            "side": side,
            "executed_qty": amount if ok else 0.0,
            "avg_price": float(r.get("avg_price", 0.0)) if isinstance(r, dict) else 0.0,
            "status": "FILLED" if ok else "ERROR",
        },
        "raw": r,  # keep raw for debugging
    }
    if not ok:
        normalized["error"] = (r.get("message") or r.get("error") or "unknown") if isinstance(r, dict) else str(r)

    return ok, normalized
