#!/usr/bin/env python3
# ops_sign_and_enqueue.py — signed client for /ops/enqueue (BUY uses quote spend; SELL uses base size)
import argparse, hashlib, hmac, json, sys
from datetime import datetime, timezone
import requests

def hmac_hex(secret: str, raw: bytes) -> str:
    return hmac.new(secret.encode("utf-8"), raw, hashlib.sha256).hexdigest()

def norm_venue(v: str) -> str:
    # accept "BINANCEUS", "BINANCE.US", "binanceus" etc.
    v = (v or "").upper().replace(" ", "")
    return v.replace(".", "")

def norm_symbol(s: str) -> str:
    s = (s or "").upper().strip()
    # allow BTCUSDT or BTC/USDT; normalize to BTC/USDT for the bus; executors will map to venue formats
    if "/" not in s and len(s) >= 6:
        # naive split assuming 3+3 (most majors); if user passed BTCUSDT -> BTC/USDT
        base = s[:-4] if s.endswith("USDT") or s.endswith("USDC") else s[:3]
        quote = s[len(base):]
        if quote:
            return f"{base}/{quote}"
    return s

def main():
    p = argparse.ArgumentParser(description="Sign & enqueue a command to NovaTrade Bus")
    p.add_argument("--base",   required=True, help="Base URL, e.g. https://novatrade3-0.onrender.com")
    p.add_argument("--secret", required=True, help="OUTBOX_SECRET (hex string)")
    p.add_argument("--agent",  required=True, help="Target agent_id (e.g. edge-cb-1)")
    p.add_argument("--venue",  required=True, help="COINBASE | BINANCEUS | KRAKEN | MEXC ...")
    p.add_argument("--symbol", required=True, help="e.g. BTC/USDT or BTCUSDT (normalized to BASE/QUOTE)")
    p.add_argument("--side",   required=True, choices=["BUY","SELL","buy","sell"])
    # amounts
    p.add_argument("--amount",       type=float, default=0.0, help="BUY: quote spend (e.g. 10 = $10 USDT/USDC)")
    p.add_argument("--base-amount",  type=float, default=0.0, dest="base_amount", help="SELL: base size (e.g. 0.0001 BTC)")
    # optional explicit quote amount (alias)
    p.add_argument("--quote",        type=float, default=None, help="Alias for BUY quote spend (prefer --amount)")
    # order type
    p.add_argument("--mode",         default="MARKET", help="MARKET (default) or LIMIT (if supported)")
    # optional idempotency key (if omitted, server/edge may derive their own)
    p.add_argument("--cid",          default=None, help="Optional client/idempotency key")
    args = p.parse_args()

    side = args.side.upper()
    venue = norm_venue(args.venue)
    symbol = norm_symbol(args.symbol)
    mode = (args.mode or "MARKET").upper()

    # ---- basic validations
    if side == "BUY":
        spend = args.quote if args.quote is not None else args.amount
        if spend is None or spend <= 0:
            print("error: BUY requires --amount (quote spend) > 0", file=sys.stderr)
            sys.exit(1)
        if args.base_amount and args.base_amount > 0:
            print("error: do not pass --base-amount for BUY", file=sys.stderr)
            sys.exit(1)
    else:  # SELL
        if args.base_amount is None or args.base_amount <= 0:
            print("error: SELL requires --base-amount (base size) > 0", file=sys.stderr)
            sys.exit(1)
        # ignore any incidental --amount/--quote on SELL to avoid mixed semantics

    # ---- payload construction
    body = {
        "agent_id": args.agent,
        "venue":    venue,
        "symbol":   symbol,
        "side":     side,
        "mode":     mode,
    }
    if side == "BUY":
        spend = args.quote if args.quote is not None else args.amount
        body["amount"] = float(spend)                 # quote spend
    else:
        body["base_amount"] = float(args.base_amount) # base size

    if args.cid:
        body["client_id"] = str(args.cid)

    raw = json.dumps(body, separators=(",", ":"), sort_keys=True).encode("utf-8")
    sig = hmac_hex(args.secret, raw)

    url = args.base.rstrip("/") + "/ops/enqueue"
    headers = {"Content-Type": "application/json", "X-Signature": sig}

    try:
        r = requests.post(url, data=raw, headers=headers, timeout=25)
        ct = (r.headers.get("content-type") or "")
        ts = datetime.now(timezone.utc).isoformat()
        if "json" in ct:
            print(f"[{ts}] [HTTP {r.status_code}] {r.json()}")
        else:
            print(f"[{ts}] [HTTP {r.status_code}] {r.text}")
        if r.status_code == 200:
            print("OK → enqueued.")
            sys.exit(0)
        else:
            sys.exit(2)
    except Exception as e:
        print("request error:", e)
        sys.exit(3)

if __name__ == "__main__":
    main()
