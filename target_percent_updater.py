import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os

def run_target_percent_updater():
    print("📊 Updating Target % from Suggested % in Portfolio_Targets...")

    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
        client = gspread.authorize(creds)
        sheet = client.open_by_url(os.getenv("SHEET_URL"))
        ws = sheet.worksheet("Portfolio_Targets")

        rows = ws.get_all_records()
        updates = 0

        for i, row in enumerate(rows, start=2):
            try:
                target_cell = f"C{i}"  # Target %
                suggested_cell = f"G{i}"  # Suggested Target %

                target = float(str(row.get("Target %", 0)).strip() or 0)
                suggested = float(str(row.get("Suggested Target %", 0)).strip() or 0)

                if suggested > 0 and abs(suggested - target) >= 0.01:
                    ws.update_acell(target_cell, suggested)
                    updates += 1
                    print(f"✅ Updated {row.get('Token', '')}: {target}% → {suggested}%")
            except Exception as err:
                print(f"⚠️ Could not update row {i}: {err}")

        print(f"✅ Target % update complete. {updates} tokens adjusted.")

    except Exception as e:
        print(f"❌ Error updating Target %: {e}")
