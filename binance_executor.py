import os
from dotenv import load_dotenv
load_dotenv()
import time
from binance.client import Client
from binance.exceptions import BinanceAPIException
from datetime import datetime
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# === Binance API Check ===
def is_binance_accessible():
    try:
        client = Client(os.getenv("BINANCE_API_KEY"), os.getenv("BINANCE_API_SECRET"))
        client.ping()
        return client
    except BinanceAPIException as e:
        print(f"❌ Binance API unavailable: {e.message}")
        return None
    except Exception as e:
        print(f"❌ Unexpected Binance connection error: {str(e)}")
        return None

client = is_binance_accessible()
if not client:
    print("⚠️ Binance not accessible. Local engine aborting.")
    exit()

# === Google Sheets Setup ===
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
gclient = gspread.authorize(creds)
sheet = gclient.open_by_url(os.getenv("SHEET_URL"))
trade_log = sheet.worksheet("Trade_Log")

BUY_ALLOCATION_PCT = 10

def log_trade(token, action, qty, price, usdt_value, alloc_pct, status):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    row = [now, token.upper(), action, qty, price, usdt_value, alloc_pct, status]
    trade_log.append_row(row, value_input_option="USER_ENTERED")
    print(f"✅ Trade logged: {action} {token}")

def execute_buy(token):
    try:
        symbol = f"{token.upper()}USDT"
        balance = float(client.get_asset_balance(asset="USDT")["free"])
        spend = round(balance * (BUY_ALLOCATION_PCT / 100), 2)
        if spend < 5:
            print(f"⚠️ Not enough USDT to buy {token}. Skipping.")
            return
        price = float(client.get_symbol_ticker(symbol=symbol)["price"])
        qty = round(spend / price, 5)
        client.order_market_buy(symbol=symbol, quantity=qty)
        log_trade(token, "BUY", qty, price, spend, BUY_ALLOCATION_PCT, "✅ Executed")
    except BinanceAPIException as e:
        log_trade(token, "BUY", "-", "-", "-", BUY_ALLOCATION_PCT, f"❌ Binance Error: {e.message}")
    except Exception as e:
        log_trade(token, "BUY", "-", "-", "-", BUY_ALLOCATION_PCT, f"❌ Error: {str(e)}")

def execute_sell(token):
    try:
        symbol = f"{token.upper()}USDT"
        balance = float(client.get_asset_balance(asset=token.upper())["free"])
        if balance < 0.0001:
            print(f"⚠️ No {token} to sell.")
            return {
                "qty": 0,
                "price": 0,
                "usdt_value": 0,
                "allocation": "100%",
                "status": "⚠️ No balance",
                "roi_target": "",
                "exit_roi": ""
            }
        price = float(client.get_symbol_ticker(symbol=symbol)["price"])
        qty = round(balance, 5)
        client.order_market_sell(symbol=symbol, quantity=qty)
        usdt_value = round(qty * price, 2)
        log_trade(token, "SELL", qty, price, usdt_value, 100, "✅ Executed")
        return {
            "qty": qty,
            "price": price,
            "usdt_value": usdt_value,
            "allocation": "100%",
            "status": "✅ Executed",
            "roi_target": "",
            "exit_roi": ""
        }
    except BinanceAPIException as e:
        log_trade(token, "SELL", "-", "-", "-", 100, f"❌ Binance Error: {e.message}")
        return {
            "qty": "-",
            "price": "-",
            "usdt_value": "-",
            "allocation": "100%",
            "status": f"❌ Binance Error: {e.message}",
            "roi_target": "",
            "exit_roi": ""
        }
    except Exception as e:
        log_trade(token, "SELL", "-", "-", "-", 100, f"❌ Error: {str(e)}")
        return {
            "qty": "-",
            "price": "-",
            "usdt_value": "-",
            "allocation": "100%",
            "status": f"❌ Error: {str(e)}",
            "roi_target": "",
            "exit_roi": ""
        }

def get_usdt_balance():
    try:
        account_info = client.get_account()
        for balance in account_info["balances"]:
            if balance["asset"] == "USDT":
                return float(balance["free"])
        return 0.0
    except Exception as e:
        print(f"❌ Error fetching USDT balance: {e}")
        return 0.0
