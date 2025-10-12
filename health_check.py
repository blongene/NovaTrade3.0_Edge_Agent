import os
import gspread
from oauth2client.service_account import ServiceAccountCredentials

REQUIRED_TABS = [
    "Scout Decisions", "Rotation_Planner", "Rotation_Log", "ROI_Review_Log",
    "Claim_Tracker", "NovaHeartbeat", "Portfolio_Targets", "Token_Vault"
]

def run_health_check():
    try:
        print("🩺 Running health check...")
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
        client = gspread.authorize(creds)
        sheet = client.open_by_url(os.getenv("SHEET_URL"))
        tabs = [ws.title for ws in sheet.worksheets()]

        missing = [tab for tab in REQUIRED_TABS if tab not in tabs]
        if missing:
            print(f"❌ Missing tabs: {missing}")
        else:
            print("✅ All required tabs found.")
    except Exception as e:
        print(f"❌ Health check failed: {e}")