import time
import os
from trade_watcher import run_once

def start_trade_watcher():
    print("Sheet executor enabled. Starting trade watcher.")
    while True:
        try:
            n = run_once()
            if n:
                print(f"Executed {n} trade(s).")
        except Exception as e:
            print("Loop error:", e)
        time.sleep(10)

def main():
    print("🚀 NovaTrade MEXC Engine Live")
    if os.getenv("ENABLE_SHEET_EXECUTOR", "false").lower() == "true":
        start_trade_watcher()
    else:
        print("Sheet executor disabled (ENABLE_SHEET_EXECUTOR=false). Using Command Bus only.")
        # Keep the main thread alive if you have other tasks,
        # otherwise, the script will exit.
        while True:
            time.sleep(3600)

if __name__ == "__main__":
    main()
