import os
from dotenv import load_dotenv

load_dotenv(dotenv_path=os.path.join(os.getcwd(), ".env"))

SHEET_URL = os.getenv("SHEET_URL","").strip()
CREDS_FILE = os.getenv("GOOGLE_CREDS_FILE","sentiment-log-service.json").strip()

EXCHANGE = os.getenv("EXCHANGE","COINBASE").upper()
MEXC_API_KEY = os.getenv("MEXC_API_KEY","").strip()
MEXC_API_SECRET = os.getenv("MEXC_API_SECRET","").strip()

MIN_USDT_ORDER = float(os.getenv("MIN_USDT_ORDER","1"))
DEFAULT_BUY_USDT = float(os.getenv("DEFAULT_BUY_USDT","10"))
