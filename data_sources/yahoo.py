import os
from datetime import datetime, timedelta

import duckdb
import pandas as pd
import yfinance as yf
from tqdm import tqdm

from lib import chunk_list

DATA_DIR_YAHOO = "yahoo_data"
os.makedirs(DATA_DIR_YAHOO, exist_ok=True)


def download(tickers, start_date=datetime(2025, 1, 1), data_dir=DATA_DIR_YAHOO):
    for batch in tqdm(
        list(chunk_list(tickers, 100)), desc="Downloading batches"
    ):  # Yahoo limit is 100 per request
        try:
            data = yf.download(
                tickers=" ".join(batch),
                start=start_date,
                end=datetime.today() + timedelta(days=1),  # yfinance end is exclusive
                group_by="ticker",
                auto_adjust=True,
                threads=True,
                progress=False,
            )
            for ticker in batch:
                if (
                    isinstance(data.columns, pd.MultiIndex)
                    and ticker in data.columns.levels[0]
                ):
                    ticker_data = data[ticker].dropna(how="all")
                elif not isinstance(
                    data.columns, pd.MultiIndex
                ):  # Single ticker edge case
                    ticker_data = data.dropna(how="all")
                else:
                    continue
                if not ticker_data.empty:
                    file_path = os.path.join(data_dir, f"{ticker}.csv")
                    ticker_data.to_csv(file_path)
        except Exception as e:
            print(f"Batch failed: {e}")


def consolidate(data_dir=DATA_DIR_YAHOO):
    query = f"""
    SELECT
        regexp_replace(filename, '^.*/|\\.csv$', '', 'g') AS symbol,
        t.Date AS date,
        t.Open AS open,
        t.Close AS close,
        t.High AS high,
        t.Low AS low,
        t.Volume AS volume
    FROM '{data_dir}/*.csv' t
    """
    df = duckdb.query(query).to_df()

    df = df.sort_values(["symbol", "date"])

    df["date"] = pd.to_datetime(df["date"])

    df = df.set_index("date")

    return df
