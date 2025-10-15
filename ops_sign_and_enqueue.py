#!/usr/bin/env python3
# ops_sign_and_enqueue.py — signed client for /ops/enqueue
# BUY uses --amount (quote spend). SELL uses --base-amount (base size).
import argparse, hashlib, hmac, json, sys
from datetime import datetime, timezone
import requests

def hmac_hex(secret: str, raw: bytes) -> str:
    return hmac.new(secret.encode("utf-8"), raw, hashlib.sha256).hexdigest()

def norm_venue(v: str) -> str:
    v = (v or "").upper().replace(" ", "")
    return v.replace(".", "")  # e.g. BINANCE.US -> BINANCEUS

def norm_symbol(s: str) -> str:
    s = (s or "").upper().strip()
    if "/" not in s and len(s) >= 6:
        # BTCUSDT -> BTC/USDT ; BTCUSDC -> BTC/USDC ; else fallback
        if s.endswith(("USDT","USDC")):
            return f"{s[:-4]}/{s[-4:]}"
        return f"{s[:3]}/{s[3:]}"
    return s

def main():
    p = argparse.ArgumentParser(description="Sign & enqueue a command to NovaTrade Bus")
    p.add_argument("--base",   required=True, help="Base URL, e.g. https://novatrade3-0.onrender.com")
    p.add_argument("--secret", required=True, help="OUTBOX_SECRET (hex)")
    p.add_argument("--agent",  required=True, help="Target agent_id (e.g. edge-cb-1)")
    p.add_argument("--venue",  required=True, help="COINBASE | BINANCEUS | KRAKEN | MEXC ...")
    p.add_argument("--symbol", required=True, help="e.g. BTC/USDT or BTCUSDT (normalized to BASE/QUOTE)")
    p.add_argument("--side",   required=True, choices=["BUY","SELL","buy","sell"])
    # BUY (quote spend)
    p.add_argument("--amount",      type=float, default=0.0, help="BUY: quote spend (e.g., 10 = $10)")
    p.add_argument("--quote",       type=float, default=None, help="Alias for BUY quote spend (prefer --amount)")
    # SELL (base size) — accept both hyphen and underscore forms
    p.add_argument("--base-amount", type=float, default=0.0, dest="base_amount",
                   help="SELL: base size (e.g., 0.0001 BTC)")
    p.add_argument("--base_amount", type=float, default=None,
                   help="SELL: base size (alias of --base-amount)")
    # order type
    p.add_argument("--mode",        default="MARKET", help="MARKET (default) or LIMIT (if supported)")
    # optional idempotency key
    p.add_argument("--cid",         default=None, help="Optional client/idempotency key")
    args = p.parse_args()

    side = args.side.upper()
    venue = norm_venue(args.venue)
    symbol = norm_symbol(args.symbol)
    mode = (args.mode or "MARKET").upper()

    # coalesce base_amount aliases
    base_amt = args.base_amount if args.base_amount else (args.base_amount if args.base_amount else 0.0)
    if args.base_amount is None and args.base_amount is None and args.base_amount is not None:
        base_amt = args.base_amount  # defensive; effectively no-op

    # ---- basic validations
    if side == "BUY":
        spend = args.quote if args.quote is not None else args.amount
        if spend is None or spend <= 0:
            print("error: BUY requires --amount (quote spend) > 0", file=sys.stderr)
            sys.exit(1)
        if (args.base_amount or args.base_amount) and base_amt > 0:
            print("error: do not pass --base-amount for BUY", file=sys.stderr)
            sys.exit(1)
    else:
        # SELL
        base_amt = args.base_amount if args.base_amount else (args.base_amount or 0.0)
        if base_amt <= 0:
            print("error: SELL requires --base-amount (base size) > 0", file=sys.stderr)
            sys.exit(1)

    # ---- payload
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
        body["base_amount"] = float(base_amt)         # base size

    if args.cid:
        body["client_id"] = str(args.cid)

    # canonical sorted JSON for HMAC
    raw_sorted = json.dumps(body, separators=(",", ":"), sort_keys=True).encode("utf-8")
    sig = hmac_hex(args.secret, raw_sorted)

    url = args.base.rstrip("/") + "/ops/enqueue"
    headers = {"Content-Type": "application/json", "X-Signature": sig}
    try:
        r = requests.post(url, data=raw_sorted, headers=headers, timeout=25)
        ts = datetime.now(timezone.utc).isoformat()
        if "json" in (r.headers.get("content-type") or ""):
            print(f"[{ts}] [HTTP {r.status_code}] {r.json()}")
        else:
            print(f"[{ts}] [HTTP {r.status_code}] {r.text}")
        if r.status_code == 200:
            print("OK → enqueued.")
            sys.exit(0)
        sys.exit(2)
    except Exception as e:
        print("request error:", e)
        sys.exit(3)

if __name__ == "__main__":
    main()
