# rebalance_handler.py

import gspread
import os
import time
from datetime import datetime
from oauth2client.service_account import ServiceAccountCredentials
from mexc_executor import execute_sell

def run_rebalance_handler():
    print("🔁 Rebalance Handler active...")
    
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
    client = gspread.authorize(creds)

    sheet = client.open_by_url(os.getenv("SHEET_URL"))
    planner_ws = sheet.worksheet("Rotation_Planner")
    log_ws = sheet.worksheet("Trade_Log")

    planner_data = planner_ws.get_all_records()
    executed = log_ws.get_all_records()

    for i, row in enumerate(planner_data, start=2):
        token = row.get("Token", "").strip()
        response = str(row.get("User Response", "")).strip().upper()
        source = str(row.get("Source", "")).strip()
        confirmed = str(row.get("Confirmed", "")).strip().upper()

        if response == "YES" and source == "Rebalance_Overweight" and confirmed != "EXECUTED":
            print(f"📉 Executing rebalance SELL for {token}...")

            try:
                execute_sell(token)  # Sell 100% of position
                planner_ws.update_acell(f"D{i}", "EXECUTED")
                timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                log_ws.append_row([
                    timestamp, token, "SELL", "Rebalance_Overweight", "Auto", "100%", "", "", ""
                ])
                print(f"✅ {token} sell logged to Trade_Log.")
            except Exception as e:
                print(f"❌ Sell failed for {token}: {e}")

            time.sleep(2)

    print("✅ Rebalance Handler scan complete.")
