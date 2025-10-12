import time
from trade_watcher import run_once

# in trade_main.py (or where the sheet watcher starts)
if os.getenv("ENABLE_SHEET_EXECUTOR", "false").lower() == "true":
    # start sheet-driven watcher
    start_trade_watcher()
else:
    print("Sheet executor disabled (ENABLE_SHEET_EXECUTOR=false). Using Command Bus only.")

def main():
    print("🚀 NovaTrade MEXC Engine Live")
    while True:
        try:
            n = run_once()
            if n:
                print(f"Executed {n} trade(s).")
        except Exception as e:
            print("Loop error:", e)
        time.sleep(10)

if __name__ == "__main__":
    main()
