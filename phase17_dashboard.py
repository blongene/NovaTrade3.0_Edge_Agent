import os, pandas as pd
from dotenv import load_dotenv
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import plotly.graph_objects as go

load_dotenv()

SHEET_URL = os.getenv("SHEET_URL","").strip()
CREDS_FILE = os.getenv("GOOGLE_CREDS_FILE","sentiment-log-service.json").strip()
START_CAPITAL = float(os.getenv("START_CAPITAL","5000"))

def _gclient():
    scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name(CREDS_FILE, scope)
    return gspread.authorize(creds)

def _from_sheet():
    gc = _gclient()
    sh = gc.open_by_url(SHEET_URL)
    log = pd.DataFrame(sh.worksheet("Trade_Log").get_all_records())
    return log

def _demo():
    # tiny demo dataset
    return pd.DataFrame([
        {"Timestamp":"2025-01-01 00:00:00","Action":"BUY","Token":"ABC","usdt_value":100},
        {"Timestamp":"2025-01-05 00:00:00","Action":"SELL","Token":"ABC","usdt_value":120},
        {"Timestamp":"2025-02-01 00:00:00","Action":"BUY","Token":"XYZ","usdt_value":150},
        {"Timestamp":"2025-02-20 00:00:00","Action":"SELL","Token":"XYZ","usdt_value":200},
    ])

def build(df: pd.DataFrame, out_html="dashboard.html"):
    if df.empty or "usdt_value" not in df.columns:
        df = _demo()
    df["Timestamp"] = pd.to_datetime(df["Timestamp"])
    df = df.sort_values("Timestamp")

    # naive equity curve using usdt_value sign by action
    df["pnl"] = df.apply(lambda r: (r["usdt_value"] if str(r["Action"]).upper()=="SELL" else -float(r["usdt_value"])), axis=1)
    df["equity"] = START_CAPITAL + df["pnl"].cumsum()

    fig = go.Figure()
    fig.add_trace(go.Scatter(x=df["Timestamp"], y=df["equity"], mode="lines+markers", name="Equity"))
    fig.update_layout(title="Compound ROI Dashboard", xaxis_title="Time", yaxis_title="Equity (USDT)")
    fig.write_html(out_html, include_plotlyjs="cdn")
    print(f"Wrote {out_html}")

if __name__=="__main__":
    try:
        df = _from_sheet()
    except Exception:
        df = pd.DataFrame()
    build(df)
