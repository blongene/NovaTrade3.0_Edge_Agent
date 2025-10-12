
import time
from rebalance_engine import run_rebalance_engine

if __name__ == "__main__":
    while True:
        run_rebalance_engine()
        time.sleep(3600)  # Wait 1 hour
