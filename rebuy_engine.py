# rebuy_engine.py

import os
import gspread
from datetime import datetime
from oauth2client.service_account import ServiceAccountCredentials
from mexc_executor import execute_buy, get_usdt_balance

def run_undersized_rebuy():
    print("📉 Checking for undersized positions...")
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
    client = gspread.authorize(creds)
    sheet = client.open_by_url(os.getenv("SHEET_URL"))

    targets_ws = sheet.worksheet("Portfolio_Targets")
    log_ws = sheet.worksheet("Rotation_Log")

    targets = targets_ws.get_all_records()
    holdings = log_ws.get_all_records()

    token_to_actual = {}
    for row in holdings:
        token = row["Token"]
        alloc = row["Allocation"].replace("%", "").strip()
        try:
            token_to_actual[token.upper()] = float(alloc)
        except:
            continue

    drift_tokens = []
    for row in targets:
        token = row["Token"].strip().upper()
        target_pct = float(row.get("Target %", 0))
        actual_pct = token_to_actual.get(token, 0.0)
        drift = round(target_pct - actual_pct, 2)

        if drift > 1.0:
            drift_tokens.append((token, drift))

    if not drift_tokens:
        print("✅ No undersized tokens found.")
        return

    usdt_balance = get_usdt_balance()
    if usdt_balance < 10:
        print("⚠️ Not enough USDT to rebalance. Skipping.")
        return

    # Allocate USDT proportionally based on drift
    total_drift = sum(d[1] for d in drift_tokens)
    for token, drift in drift_tokens:
        portion = drift / total_drift
        buy_amount = round(usdt_balance * portion, 2)
        if buy_amount < 5:
            print(f"⏩ Skipping {token}: too small ({buy_amount} USDT)")
            continue
        print(f"💰 Rebalancing {token} with {buy_amount} USDT...")
        execute_buy(token, buy_amount)

    print("✅ Rebuy engine complete.")
