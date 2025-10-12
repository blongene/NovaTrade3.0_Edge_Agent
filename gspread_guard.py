# gspread_guard.py — wrap legacy gspread calls with Nova utils (cache + gates + backoff)
import os, time
from functools import wraps

# Pull our quota-safe wrappers
from utils import (
    _ws_get_all_values,          # gated + backoff
    _ws_get_all_records,         # gated + backoff
    ws_get_all_records_cached,   # cached by worksheet title
    ws_batch_update,
    _ws_update_cell, _ws_update_acell, _ws_append_row,
)

ENABLE   = os.getenv("NOVA_PATCH_GSPREAD", "1") == "1"
TTL_VALS = int(os.getenv("GSPREAD_GUARD_TTL_SEC", "300"))       # 5m cache for values
VERBOSE  = os.getenv("GSPREAD_GUARD_VERBOSE", "1") == "1"

# In-process per-worksheet cache keyed by (spreadsheet_id, title, kind)
_CACHE = {}  # { (id, title, "values"): (ts, data) }

def _ws_key(ws, kind="values"):
    try:
        sid = getattr(ws.spreadsheet, "id", None) or id(ws.spreadsheet)
        title = getattr(ws, "title", "")
        return (sid, title, kind)
    except Exception:
        return (id(ws), getattr(ws, "title", ""), kind)

def _cache_get(ws, kind, ttl):
    k = _ws_key(ws, kind)
    ent = _CACHE.get(k)
    if ent and (time.time() - ent[0]) < ttl:
        return ent[1]
    return None

def _cache_put(ws, kind, data):
    _CACHE[_ws_key(ws, kind)] = (time.time(), data)

def _log(msg):
    if VERBOSE:
        print(f"🧩 gspread_guard: {msg}")

def _patch():
    import gspread
    try:
        Worksheet = gspread.models.Worksheet
    except Exception:
        # Nothing to patch
        return

    # Save originals
    _orig_get_all_values   = Worksheet.get_all_values
    _orig_get_all_records  = Worksheet.get_all_records
    _orig_row_values       = getattr(Worksheet, "row_values", None)
    _orig_col_values       = getattr(Worksheet, "col_values", None)
    _orig_update_cell      = Worksheet.update_cell
    _orig_update_acell     = Worksheet.update_acell
    _orig_update           = Worksheet.update
    _orig_append_row       = Worksheet.append_row
    _orig_batch_update     = getattr(Worksheet, "batch_update", None)

    # Reads
    def get_all_values_patched(self, *args, **kwargs):
        cached = _cache_get(self, "values", TTL_VALS)
        if cached is not None:
            _log(f"get_all_values → cache hit ({self.title})")
            return cached
        _log(f"get_all_values → utils (API) ({self.title})")
        vals = _ws_get_all_values(self)
        _cache_put(self, "values", vals)
        return vals

    def get_all_records_patched(self, *args, **kwargs):
        # Use our per-title cache
        _log(f"get_all_records → utils cached ({self.title})")
        return ws_get_all_records_cached(self, ttl_s=TTL_VALS)

    def row_values_patched(self, idx, *args, **kwargs):
        vals = get_all_values_patched(self)
        i = int(idx) - 1
        return vals[i] if 0 <= i < len(vals) else []

    def col_values_patched(self, idx, *args, **kwargs):
        vals = get_all_values_patched(self)
        j = int(idx) - 1
        return [row[j] for row in vals if j < len(row)] if j >= 0 else []

    # Writes
    def update_cell_patched(self, r, c, v, *args, **kwargs):
        _log(f"update_cell → utils ({self.title}:{r},{c})")
        return _ws_update_cell(self, r, c, v)

    def update_acell_patched(self, a1, v, *args, **kwargs):
        _log(f"update_acell → utils ({self.title}:{a1})")
        return _ws_update_acell(self, a1, v)

    def update_patched(self, rng, rows, *args, **kwargs):
        _log(f"update(range) → utils.batch ({self.title}:{rng})")
        # Normalize to our batch_update signature
        return ws_batch_update(self, [{"range": rng, "values": rows}])

    def append_row_patched(self, row, *args, **kwargs):
        _log(f"append_row → utils ({self.title})")
        return _ws_append_row(self, row)

    def batch_update_patched(self, data, *args, **kwargs):
        _log(f"batch_update → utils ({self.title})")
        return ws_batch_update(self, data)

    # Install
    Worksheet.get_all_values  = get_all_values_patched
    Worksheet.get_all_records = get_all_records_patched
    if _orig_row_values: Worksheet.row_values = row_values_patched
    if _orig_col_values: Worksheet.col_values = col_values_patched
    Worksheet.update_cell     = update_cell_patched
    Worksheet.update_acell    = update_acell_patched
    Worksheet.update          = update_patched
    Worksheet.append_row      = append_row_patched
    if _orig_batch_update: Worksheet.batch_update = batch_update_patched

    _log("patched gspread.Worksheet methods (cache+gates+backoff)")

if ENABLE:
    try:
        _patch()
    except Exception as e:
        _log(f"patch failed (non-fatal): {e}")
