import os
from datetime import datetime, timedelta
from itertools import islice
from time import sleep
from dotenv import load_dotenv
import requests
import pandas as pd
from tqdm import tqdm

from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame
from alpaca.data.enums import Adjustment

load_dotenv()

ALPACA_API_KEY = os.getenv("ALPACA_API_KEY")
ALPACA_SECRET_KEY = os.getenv("ALPACA_SECRET_KEY")

PARQUET_DIR = "alpaca_data"
os.makedirs(PARQUET_DIR, exist_ok=True)

def get_symbols():
    url = "https://api.nasdaq.com/api/screener/stocks?tableonly=false&limit=25&download=true"
    response = requests.get(url, headers={"user-agent": "Mozilla/5.0"}).json()
    symbols = [
        row["symbol"].strip()
        for row in response["data"]["rows"]
        if "^" not in row["symbol"] and "/" not in row["symbol"]
    ]
    return symbols

def batcher(iterable, n):
    it = iter(iterable)
    while batch := list(islice(it, n)):
        yield batch

def get_last_saved_date(ticker):
    file_path = os.path.join(PARQUET_DIR, f"{ticker}.parquet")
    if os.path.exists(file_path):
        try:
            df = pd.read_parquet(file_path)
            if not df.empty:
                return df.index.max().date()
        except:
            pass
    return None

def update_tickers(needed_updates):
    for batch in batcher(needed_updates, 200):  # Alpaca limit is 200 per request
        try:
            request = StockBarsRequest(
                symbol_or_symbols=batch,
                timeframe=TimeFrame.Day,
                start=start_date,
                adjustment=Adjustment.ALL,
            )
            df = client.get_stock_bars(request).df
            for symbol in batch:
                if symbol in df.index.get_level_values("symbol"):
                    df_symbol = df.xs(symbol, level="symbol")
                    df_symbol.to_parquet(f"{PARQUET_DIR}/{symbol}.parquet")
        except Exception as e:
            print(f"Error processing batch {batch}: {e}")
            sleep(30)  # simple wait before retrying next batch

start_date = datetime(2025, 1, 1)
client = StockHistoricalDataClient(ALPACA_API_KEY, ALPACA_SECRET_KEY)

tickers = get_symbols()

today = datetime.today().date()
needed_updates = []

# TODO: Just download eveything from Mon - Fri

for ticker in tqdm(tickers, desc="Checking which tickers need updates"):
    last_date = get_last_saved_date(ticker)
    if last_date is None or last_date < (today - timedelta(days=1)):
        needed_updates.append(ticker)
        print(ticker, last_date)

print(len(needed_updates))

update_tickers(needed_updates)