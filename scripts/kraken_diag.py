import os, time, hmac, hashlib, base64, urllib.parse, requests
BASE = os.getenv("KRAKEN_BASE_URL","https://api.kraken.com").rstrip("/")
KEY  = os.getenv("KRAKEN_KEY","").strip()
SEC  = os.getenv("KRAKEN_SECRET","").strip()  # base64

def _sign(path, data):
    if not KEY or not SEC:
        raise SystemExit("Set KRAKEN_KEY and KRAKEN_SECRET in environment.")
    nonce = str(int(time.time()*1000))
    data = {**(data or {}), "nonce": nonce}
    postdata = urllib.parse.urlencode(data)
    sha256 = hashlib.sha256((nonce + postdata).encode()).digest()
    mac = hmac.new(base64.b64decode(SEC), (path.encode() + sha256), hashlib.sha512)
    sig = base64.b64encode(mac.digest()).decode()
    return {"hdr": {"API-Key": KEY, "API-Sign": sig}, "qs": postdata}

def k_private(path, data=None):
    s = _sign(path, data or {})
    r = requests.post(f"{BASE}{path}", data=s["qs"], headers=s["hdr"], timeout=15)
    j = r.json()
    if j.get("error"): raise SystemExit(f"Kraken error: {j['error']}")
    print("OK:", list(j["result"].keys())[:5])

if __name__ == "__main__":
    k_private("/0/private/Balance")
