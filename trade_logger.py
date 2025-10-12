# trade_logger.py — local edge, quota-calm buffered logger
import os, json, time, atexit, pathlib
from utils import get_ws, ws_batch_update, with_sheet_backoff

TAB = os.getenv("TRADE_LOG_TAB", "Trade_Log")
BUF_FILE = pathlib.Path(os.getenv("TRADE_LOG_BUF", "/tmp/nova_trade_log.json"))
MAX_BATCH = int(os.getenv("TRADE_LOG_MAX_BATCH", "200"))
FLUSH_INTERVAL_S = int(os.getenv("TRADE_LOG_FLUSH_SEC", "15"))

_buffer = []
_last_flush = 0

def _load_buf():
    global _buffer
    try:
        if BUF_FILE.exists():
            _buffer = json.loads(BUF_FILE.read_text())
    except Exception:
        _buffer = []

def _save_buf():
    try:
        BUF_FILE.write_text(json.dumps(_buffer))
    except Exception:
        pass

def _flush(force=False):
    global _buffer, _last_flush
    now = time.time()
    if not force and (now - _last_flush) < FLUSH_INTERVAL_S:
        return
    if not _buffer:
        return
    rows = _buffer[:MAX_BATCH]
    ws = get_ws(TAB)
    # append in one go; let Sheets place automatically by using "A" start
    ws_batch_update(ws, [{"range": f"A{ws.row_count+1}", "values": rows}])
    _buffer = _buffer[MAX_BATCH:]
    _save_buf()
    _last_flush = now
    print(f"🧾 Trade_Log flush: wrote {len(rows)} rows, {len(_buffer)} pending")

@with_sheet_backoff
def log_trade_row(row):
    """
    row = [ts, token, side, qty, price, usdt_value, status]
    """
    if not isinstance(row, list):
        raise ValueError("log_trade_row expects a list")
    if not _buffer:
        _load_buf()
    _buffer.append(row)
    _save_buf()
    _flush()

def flush_now():
    _flush(force=True)

atexit.register(flush_now)
