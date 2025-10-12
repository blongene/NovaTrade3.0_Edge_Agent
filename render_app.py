from flask import Flask, send_file
import os
from phase17_dashboard import build, _from_sheet
app = Flask(__name__)

@app.route("/")
def home():
    try:
        df = _from_sheet()
    except Exception:
        import pandas as pd
        df = pd.DataFrame()
    build(df, "dashboard.html")
    return send_file("dashboard.html")

if __name__ == "__main__":
    port = int(os.environ.get("PORT","5000"))
    app.run(host="0.0.0.0", port=port)
