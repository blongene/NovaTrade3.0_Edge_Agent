# rebalance_engine.py  (LOCAL VPN EXECUTOR)
import os, time
from typing import Dict, Any, Tuple

# ==== Telegram notify ====
def send_telegram(text: str):
    try:
        token = os.getenv("TELEGRAM_BOT_TOKEN") or os.getenv("BOT_TOKEN")
        chat  = os.getenv("TELEGRAM_CHAT_ID")  or os.getenv("CHAT_ID")
        if not token or not chat:
            print("ℹ️ Telegram creds missing; skip notify.")
            return
        import requests
        r = requests.post(f"https://api.telegram.org/bot{token}/sendMessage",
                          json={"chat_id": chat, "text": text, "parse_mode": "Markdown"},
                          timeout=10)
        if r.status_code != 200:
            print(f"⚠️ Telegram send failed: {r.status_code} {r.text}")
    except Exception as e:
        print(f"⚠️ Telegram send error: {e}")

# ==== Exchange executor hooks (replace with your real ones if available) ====
def execute_buy(symbol: str, qty: float) -> Dict[str, Any]:
    """
    Replace with your real MEXC/Binance executor.
    Should raise on error. Return dict with orderId/price/qty if possible.
    """
    print(f"🟢 BUY {symbol} qty={qty}")
    return {"side":"BUY","symbol":symbol,"qty":qty}

def execute_sell(symbol: str, qty: float) -> Dict[str, Any]:
    print(f"🔴 SELL {symbol} qty={qty}")
    return {"side":"SELL","symbol":symbol,"qty":qty}

# ==== Sheet I/O hooks (replace with your real implementations) ====
def read_current_weights() -> Dict[str, float]:
    # e.g., {"MIND": 6.2, "OZAKAI": 2.9, ...} from Portfolio_Actuals
    raise NotImplementedError

def read_target_weights() -> Dict[str, float]:
    # e.g., {"MIND": 5.0, "OZAKAI": 3.0, ...} from Portfolio_Targets
    raise NotImplementedError

def read_equity_usd() -> float:
    # return current portfolio equity in USD (from sheet or local calc)
    try:
        return float(os.getenv("PORTFOLIO_EQUITY_FALLBACK_USD","5000"))
    except:
        return 5000.0

def log_trade(symbol: str, side: str, usd: float, note: str = "Auto‑Rebalance"):
    # append to Trade_Log; safe no‑op if not implemented
    try:
        print(f"🧾 Log trade: {side} {symbol} ${usd:.2f} — {note}")
        # TODO: implement append_row to Trade_Log here
    except Exception as e:
        print(f"⚠️ Trade log failed: {e}")

# ==== Parameters ====
REBALANCE_BAND_PCT = float(os.getenv("REBALANCE_BAND_PCT", "20"))  # act if >20% off target
MIN_TRADE_USD      = float(os.getenv("MIN_TRADE_USD", "25"))
MAX_TRADE_USD      = float(os.getenv("MAX_TRADE_USD", "500"))
COOLDOWN_MIN       = int(os.getenv("REBALANCE_COOLDOWN_MIN", "60"))

_last_trade_ts: Dict[str, float] = {}

def _cooldown_ok(symbol: str) -> bool:
    ts = _last_trade_ts.get(symbol, 0)
    return time.time() - ts >= COOLDOWN_MIN * 60

def _mark_trade(symbol: str):
    _last_trade_ts[symbol] = time.time()

def _format_alert(symbol: str, side: str, delta_pct: float, usd: float) -> str:
    return (
        f"🔄 *Auto‑Rebalance Triggered*\n\n"
        f"*Token:* {symbol}\n"
        f"*Action:* {side}\n"
        f"*Deviation:* {delta_pct:+.1f}% vs target\n"
        f"*Size:* ${usd:.2f}\n"
        f"*Status:* EXECUTED"
    )

def _clamp_amount(usd: float) -> float:
    return max(MIN_TRADE_USD, min(MAX_TRADE_USD, usd))

def _calc_deviation(cur: float, tgt: float) -> float:
    if tgt is None or tgt == 0:
        return 0.0
    return (cur - tgt) / tgt * 100.0

def run_rebalance_once():
    equity = read_equity_usd()
    try:
        current = read_current_weights()
        target  = read_target_weights()
    except NotImplementedError:
        print("⚠️ read_current_weights/read_target_weights not implemented; skipping.")
        return

    acted = 0
    for sym, tgt_w in target.items():
        cur_w = current.get(sym, 0.0)
        delta = _calc_deviation(cur_w, tgt_w)

        # Only act if outside band
        band = abs(delta) >= REBALANCE_BAND_PCT
        if not band:
            continue
        if not _cooldown_ok(sym):
            print(f"⏳ Skip {sym}: cooldown.")
            continue

        # Determine side and rough USD
        # Example: adjust 25% of the gap between current and target
        gap = abs(cur_w - tgt_w) / 100.0 * equity
        usd = _clamp_amount(gap * 0.25)
        side = "SELL" if delta > 0 else "BUY"

        try:
            if side == "SELL":
                execute_sell(sym, usd)  # your executor may expect qty instead of USD
            else:
                execute_buy(sym, usd)
            _mark_trade(sym)
            log_trade(sym, side, usd, "Auto‑Rebalance")
            send_telegram(_format_alert(sym, side, delta, usd))
            acted += 1
        except Exception as e:
            print(f"❌ Rebalance trade failed for {sym}: {e}")

    if acted == 0:
        print("✅ Rebalance scan complete: no actions needed.")
    else:
        print(f"✅ Rebalance scan complete: {acted} trade(s) executed.")

def run_rebalance_loop():
    while True:
        run_rebalance_once()
        time.sleep(int(os.getenv("REBALANCE_SCAN_SEC","900")))  # default 15m
