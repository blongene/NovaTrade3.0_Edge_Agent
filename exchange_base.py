from typing import Dict, Any

class ExchangeExecutor:
    venue = "GENERIC"
    def place_market_quote(self, symbol: str, side: str, quote_amount: float) -> Dict[str, Any]:
        raise NotImplementedError
