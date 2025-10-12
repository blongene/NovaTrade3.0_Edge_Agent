# token_utils.py

def get_token_price(token):
    # Placeholder: Replace with actual API logic later
    return 0.00

def get_token_symbol(token):
    # Simple symbol extractor
    return token.upper()

def get_portfolio_allocation(token):
    # Temporary logic: assume 100% allocation for testing purposes
    if token.upper() == "MIND":
        return 100.0
    return 0.0
