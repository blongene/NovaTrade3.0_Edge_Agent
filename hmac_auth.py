# hmac_auth.py — HMAC signing & verification (shared with edge)
import hmac, hashlib, time

def sign(secret: str, body: bytes, ts: str) -> str:
    """
    Canonical signature: HMAC-SHA256(secret, "<unix_ts>.<raw_body>")
    - secret: shared key (OUTBOX_SECRET / EDGE_SECRET)
    - body: raw request/response bytes (exactly as sent)
    - ts: unix timestamp string (e.g., "1693612345")
    """
    msg = ts.encode() + b"." + body
    return hmac.new(secret.encode(), msg, hashlib.sha256).hexdigest()

def verify(secret: str, body: bytes, ts: str, sig: str, ttl_s: int = 180) -> bool:
    """
    Validates signature and freshness (± ttl_s).
    """
    try:
        ts_i = int(ts)
    except Exception:
        return False
    if not secret or abs(time.time() - ts_i) > ttl_s:
        return False
    expected = sign(secret, body, ts)
    return hmac.compare_digest(expected, sig or "")
