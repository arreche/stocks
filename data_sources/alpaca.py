import os
from datetime import datetime
from time import sleep

import duckdb
import pandas as pd
from alpaca.data.enums import Adjustment
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame

from lib import chunk_list

DATA_DIR_ALPACA = "alpaca_data"
os.makedirs(DATA_DIR_ALPACA, exist_ok=True)


def download(tickers, start_date=datetime(2025, 1, 1), data_dir=DATA_DIR_ALPACA):
    alpaca_api_key = os.getenv("ALPACA_API_KEY")
    alpaca_secret_key = os.getenv("ALPACA_SECRET_KEY")
    if not alpaca_api_key or not alpaca_secret_key:
        raise EnvironmentError("Missing ALPACA API keys in environment variables.")

    client = StockHistoricalDataClient(alpaca_api_key, alpaca_secret_key)

    for batch in chunk_list(tickers, 200):  # Alpaca limit is 200 per request
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
                    df_symbol.to_csv(f"{data_dir}/{symbol}.csv")
        except Exception as e:
            print(f"Error processing batch {batch}: {e}")
            sleep(30)  # simple wait before retrying next batch


def consolidate(data_dir=DATA_DIR_ALPACA):
    query = f"""
    SELECT
        regexp_replace(filename, '^.*/|\\.csv$', '', 'g') AS symbol,
        t.*
    FROM '{data_dir}/*.csv' t
    """
    df = duckdb.query(query).to_df()

    df = df.sort_values(["symbol", "timestamp"])

    df["date"] = (
        pd.to_datetime(df["timestamp"], utc=True)
        .dt.tz_convert("America/New_York")
        .dt.date
    )
    df.drop("timestamp", axis=1, inplace=True)

    df = df.set_index("date")

    return df
