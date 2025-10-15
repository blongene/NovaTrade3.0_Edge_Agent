#!/usr/bin/env python3
# ops_sign_and_enqueue.py — minimal, correct client for /ops/enqueue
import argparse, hashlib, hmac, json, sys, time
import requests

def hmac_hex(secret: str, raw: bytes) -> str:
    return hmac.new(secret.encode("utf-8"), raw, hashlib.sha256).hexdigest()

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--base",   required=True, help="Base URL, e.g. https://novatrade3-0.onrender.com")
    p.add_argument("--secret", required=True, help="OUTBOX_SECRET")
    p.add_argument("--agent",  required=True, help="Target agent_id (e.g. edge-cb-1)")
    p.add_argument("--venue",  required=True, help="COINBASE | BINANCE.US | KRAKEN | MEXC ...")
    p.add_argument("--symbol", required=True, help="e.g. BTC/USDT")
    p.add_argument("--side",   required=True, choices=["BUY","SELL","buy","sell"])
    p.add_argument('--amount', type=float, default=0.0, help='Quote currency spend for BUY')
    p.add_argument('--base-amount', type=float, default=0.0, dest='base_amount', help='Base asset size for SELL')
    p.add_argument("--quote",  type=float, default=None, help="quote_amount (optional)")
    p.add_argument("--mode",   default="MARKET")
    args = p.parse_args()

    body = {
        "agent_id": args.agent,
        "venue":    args.venue,
        "symbol":   args.symbol,
        "side":     args.side.upper(),
        "mode":     args.mode.upper(),
    }
    if args.quote is not None:
        body["quote_amount"] = float(args.quote)
    elif args.amount is not None:
        body["amount"] = float(args.amount)
    else:
        # default to amount-less market order error on server if both missing
        pass

    raw = json.dumps(body, separators=(",", ":"), sort_keys=True).encode("utf-8")
    sig = hmac_hex(args.secret, raw)

    url = args.base.rstrip("/") + "/ops/enqueue"
    headers = {"Content-Type": "application/json", "X-Signature": sig}
    try:
        r = requests.post(url, data=raw, headers=headers, timeout=20)
        ct = (r.headers.get("content-type") or "")
        print(f"[HTTP {r.status_code}] {r.text if 'json' not in ct else r.json()}")
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
