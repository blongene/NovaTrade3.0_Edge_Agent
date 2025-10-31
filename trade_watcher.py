import os, json, time
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from config import SHEET_URL, CREDS_FILE, DEFAULT_BUY_USDT, MIN_USDT_ORDER
from executors.mexc_executor import execute_buy, execute_sell

def _gclient():
    scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name(CREDS_FILE, scope)
    return gspread.authorize(creds)

def _headers(ws):
    header = ws.row_values(1)
    return {name.strip(): idx+1 for idx,name in enumerate(header)}

def run_once():
    gc = _gclient()
    sh = gc.open_by_url(SHEET_URL)
    planner = sh.worksheet("Rotation_Planner")
    log_ws = sh.worksheet("Trade_Log")
    h = _headers(planner)

    need = ["Token","User Response","Confirmed","Source","Trade Status"]
    for k in need:
        if k not in h:
            raise RuntimeError(f"Missing header '{k}' in Rotation_Planner")

    rows = planner.get_all_values()[1:]
    executed = 0
    for i, row in enumerate(rows, start=2):
        token = row[h["Token"]-1].strip()
        user_resp = row[h["User Response"]-1].strip().upper()
        confirmed = row[h["Confirmed"]-1].strip()
        source = row[h["Source"]-1].strip().lower()
        status = row[h["Trade Status"]-1].strip().upper() if row[h["Trade Status"]-1] else ""

        if status == "EXECUTED":
            continue
        if user_resp != "YES":
            continue
        if confirmed not in ("✅","YES","TRUE","1"):
            continue

        action = "BUY"
        if "stall" in source or "manual" in source or "telegram" in source or "sell" in source:
            action = "SELL"

        if action=="BUY":
            res = execute_buy(token, DEFAULT_BUY_USDT)
        else:
            res = execute_sell(token)

        ts = time.strftime("%Y-%m-%d %H:%M:%S")
        if res:
            # log
            log_ws.append_row([ts, token, action, res.get("qty",""), res.get("price",""), res.get("usdt_value",""), "OK"], value_input_option="RAW")
            # mark executed
            planner.update_cell(i, h["Trade Status"], "EXECUTED")
            executed += 1
        else:
            log_ws.append_row([ts, token, action, "", "", "", "FAILED"], value_input_option="RAW")
    return executed
