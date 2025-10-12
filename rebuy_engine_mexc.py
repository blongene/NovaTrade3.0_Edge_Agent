# Placeholder: scans optional 'Rebuy_Queue' sheet if present and calls mexc_executor.execute_buy
import gspread, time
from oauth2client.service_account import ServiceAccountCredentials
from config import SHEET_URL, CREDS_FILE, DEFAULT_BUY_USDT
from mexc_executor import execute_buy

def _gclient():
    scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name(CREDS_FILE, scope)
    return gspread.authorize(creds)

def run_rebuy_once():
    gc = _gclient()
    sh = gc.open_by_url(SHEET_URL)
    try:
        ws = sh.worksheet("Rebuy_Queue")
    except:
        return 0
    header = {v.strip():i+1 for i,v in enumerate(ws.row_values(1))}
    need = ["Token","Status"]
    if not all(k in header for k in need):
        return 0
    rows = ws.get_all_values()[1:]
    acted = 0
    for i, r in enumerate(rows, start=2):
        if (r[header["Status"]-1] or "").upper() == "DONE":
            continue
        token = r[header["Token"]-1].strip()
        if not token:
            continue
        res = execute_buy(token, DEFAULT_BUY_USDT)
        ws.update_cell(i, header["Status"], "DONE" if res else "FAILED")
        acted += 1 if res else 0
    return acted
