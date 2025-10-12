import os, requests
from mexc_executor import _balances as balances

def diag():
    try:
        bals = balances()
        usdt = next((float(b.get("free",0) or 0) for b in bals if b.get("asset")=="USDT"), 0.0)
        print(f"💰 Free USDT: {usdt}")
    except Exception as e:
        print("Diagnostics failed:", e)

if __name__ == "__main__":
    diag()
