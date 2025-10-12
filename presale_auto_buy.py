# presale_auto_buy.py

import gspread
from oauth2client.service_account import ServiceAccountCredentials
from mexc_executor import execute_buy
import os
from datetime import datetime

def run_presale_auto_buy():
    print("🤖 Checking for presale YES votes to auto-buy...")
    
    # Auth
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
    client = gspread.authorize(creds)
    sheet = client.open_by_url(os.getenv("SHEET_URL"))
    planner_ws = sheet.worksheet("Rotation_Planner")
    trade_log_ws = sheet.worksheet("Trade_Log")
    
    planner = planner_ws.get_all_records()
    trade_log = trade_log_ws.get_all_records()

    traded_tokens = {row["Token"].strip().upper() for row in trade_log if row.get("Action") == "BUY"}

    for i, row in enumerate(planner, start=2):
        token = row.get("Token", "").strip()
        confirmed = str(row.get("Confirmed", "")).strip().upper()
        source = str(row.get("Source", "")).strip()
        
        if (
            confirmed == "YES"
            and source == "Presale Alert"
            and token.upper() not in traded_tokens
        ):
            print(f"💰 Auto-buy triggered for {token} (Presale YES vote)")
            execute_buy(token)
            traded_tokens.add(token.upper())

    print("✅ Presale auto-buy scan complete.")
