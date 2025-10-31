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

# ---- MEXC ----
    if venue == "MEXC":
        from executors.mexc_executor import place_market_order as mexc_exec
        client_order_id = payload.get("client_order_id") or f"EDGE-{int(time.time()*1000)}"
        rec = mexc_exec(
            client_order_id=client_order_id,
            symbol=symbol,
            side=side,
            amount=str(quote)
        )
        return _as_ok(rec)

    return {"status": "error", "message": f"unknown venue: {venue}", "fills": []}
