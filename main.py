import requests
import yfinance as yf
import pandas as pd
import os
from datetime import datetime, timedelta
from tqdm import tqdm

PARQUET_DIR = "parquet_data"
os.makedirs(PARQUET_DIR, exist_ok=True)

def get_symbols():
    url = 'https://api.nasdaq.com/api/screener/stocks?tableonly=false&limit=25&download=true'
    response = requests.get(url, headers={'user-agent': 'Mozilla/5.0'}).json()
    symbols = [row['symbol'].strip() for row in response['data']['rows']
              if '^' not in row['symbol'] and '/' not in row['symbol']]

    return symbols

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

def save_ticker_data(ticker, new_data):
    file_path = os.path.join(PARQUET_DIR, f"{ticker}.parquet")
    if os.path.exists(file_path):
        existing = pd.read_parquet(file_path)
        combined = pd.concat([existing, new_data])
        combined = combined[~combined.index.duplicated(keep="last")].sort_index()
    else:
        combined = new_data
    combined.to_parquet(file_path)

def chunk_list(lst, size):
    for i in range(0, len(lst), size):
        yield lst[i:i + size]

def update_all_tickers(tickers, start_date, end_date, batch_size=100):
    today = datetime.today().date()
    needed_updates = []

    # Step 1: Check which tickers are outdated
    for ticker in tqdm(tickers, desc="Checking which tickers need updates"):
        last_date = get_last_saved_date(ticker)
        if last_date is None or last_date < (today - timedelta(days=1)):
            needed_updates.append(ticker)

    # Step 2: Download data in batches
    for batch in tqdm(list(chunk_list(needed_updates, batch_size)), desc="Downloading batches"):
        try:
            data = yf.download(
                tickers=" ".join(batch),
                start=start_date,
                end=end_date + timedelta(days=1),  # yfinance end is exclusive
                group_by="ticker",
                auto_adjust=True,
                threads=True,
                progress=False
            )
            # Step 3: Split and save per ticker
            for ticker in batch:
                if isinstance(data.columns, pd.MultiIndex) and ticker in data.columns.levels[0]:
                    ticker_data = data[ticker].dropna(how="all")
                elif not isinstance(data.columns, pd.MultiIndex):  # Single ticker edge case
                    ticker_data = data.dropna(how="all")
                else:
                    continue
                if not ticker_data.empty:
                    save_ticker_data(ticker, ticker_data)
        except Exception as e:
            print(f"Batch failed: {e}")

# Example usage
tickers = get_symbols()
start_date = datetime(2025, 1, 1)
end_date = datetime.today()

update_all_tickers(tickers, start_date, end_date)
