import glob
import os

import requests
from huggingface_hub import HfApi


def get_symbols():
    url = "https://api.nasdaq.com/api/screener/stocks?tableonly=false&limit=25&download=true"
    response = requests.get(url, headers={"user-agent": "Mozilla/5.0"}).json()
    symbols = [
        row["symbol"].strip()
        for row in response["data"]["rows"]
        if "^" not in row["symbol"] and "/" not in row["symbol"]
    ]

    return symbols


def chunk_list(lst, size):
    for i in range(0, len(lst), size):
        yield lst[i : i + size]


def add_metadata(df):
    df["prev_close"] = df.groupby("symbol")["close"].shift(1)
    df["change_pct"] = (
        (df["close"] - df["prev_close"]) / df["prev_close"] * 100
    ).round(2)
    df["gap_pct"] = ((df["open"] - df["prev_close"]) / df["prev_close"] * 100).round(2)
    df["run_pct"] = ((df["high"] - df["open"]) / df["open"] * 100).round(2)
    df["avg_volume_20d"] = df.groupby("symbol")["volume"].transform(
        lambda x: x.rolling(window=20, min_periods=1).mean()
    )
    df["relative_volume"] = (df["volume"] / df["avg_volume_20d"]).round(2)
    return df


def upload_hf():
    hf_token = os.getenv("HF_TOKEN")
    if not hf_token:
        raise EnvironmentError("Missing HF TOKEN in environment variables.")

    api = HfApi(token=hf_token)

    for file in glob.glob("*.parquet"):
        api.upload_file(
            path_or_fileobj=file,
            path_in_repo=file,
            repo_id="Arrechenash/stocks",
            repo_type="dataset",
        )
