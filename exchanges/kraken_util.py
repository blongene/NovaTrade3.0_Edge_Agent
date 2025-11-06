# edge/exchanges/kraken_util.py (new or existing helpers file)

import re

_PAIR_RE = re.compile(r"^([A-Z0-9]+)[-/]?([A-Z0-9]+)$")

# Kraken altname mapping
_KRAKEN_ALT = {
    "BTC": "XBT",
    "ETH": "XETH",
    # many others map 1:1; add more if needed:
    "USDT": "USDT",
    "USDC": "USDC",
    "SOL": "SOL",
    "ADA": "ADA",
}

def parse_pair(sym: str):
    m = _PAIR_RE.match(sym.upper().replace(":", "").replace(".", ""))
    if not m:
        raise ValueError(f"bad symbol: {sym}")
    return m.group(1), m.group(2)

def to_kraken_altname(sym: str) -> str:
    base, quote = parse_pair(sym)
    base = _KRAKEN_ALT.get(base, base)
    quote = _KRAKEN_ALT.get(quote, quote)
    return f"{base}{quote}"
