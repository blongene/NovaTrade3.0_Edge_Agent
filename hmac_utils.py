# hmac_utils.py — The Single Source of Truth for Signing
import os, hmac, hashlib, json

# Allow overriding secret env var name
def get_secret(env_var="OUTBOX_SECRET"):
    s = os.getenv(env_var, "")
    # Fallback for Edge
    if not s and env_var == "OUTBOX_SECRET":
        s = os.getenv("EDGE_SECRET", "")
    return s

def canonical_bytes(body: dict) -> bytes:
    """
    Produce the Canonical JSON bytes:
    - No spaces (separators=(',',':'))
    - Sorted keys (sort_keys=True)
    This MUST match the Bus's _verify_robust logic.
    """
    return json.dumps(body, separators=(",", ":"), sort_keys=True).encode("utf-8")

def sign(body: dict, secret: str) -> str:
    """Generate HMAC-SHA256 signature."""
    if not secret: return ""
    msg = canonical_bytes(body)
    return hmac.new(secret.encode("utf-8"), msg, hashlib.sha256).hexdigest()

def get_auth_headers(body: dict, secret: str) -> dict:
    """Return the full headers dict for requests."""
    sig = sign(body, secret)
    return {
        "Content-Type": "application/json",
        "X-Nova-Signature": sig,  # Modern standard header
        "X-NT-Sig": sig           # Legacy alias
    }
