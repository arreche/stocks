import duckdb
import pandas as pd

url = "stock_prices_alpaca.parquet"

query = f"""
SELECT *
FROM read_parquet('{url}')
"""

df = duckdb.query(query).to_df()
df = df.sort_values(['symbol', 'timestamp'])

# Assuming the timestamp column is named 'timestamp'
df['timestamp'] = pd.to_datetime(df['timestamp'])
df = df.set_index('timestamp')

# Check if index is timezone-aware
if df.index.tz is None:
    # Timestamps are naive, localize them to UTC
    df = df.tz_localize('UTC')
else:
    # Timestamps are already timezone-aware; no localization needed
    pass

# Convert to America/New_York timezone
df = df.tz_convert('America/New_York')

# Add date column
df['date'] = df.index.date

# Continue with the rest of your processing...
df["prev_close"] = df.groupby("symbol")["close"].shift(1)
df["change_pct"] = ((df["close"] - df["prev_close"]) / df["prev_close"] * 100).round(2)
df["gap_pct"] = ((df["open"] - df["prev_close"]) / df["prev_close"] * 100).round(2)
df["run_pct"] = ((df["high"] - df["open"]) / df["open"] * 100).round(2)
df["avg_volume_20d"] = df.groupby("symbol")["volume"].transform(
    lambda x: x.rolling(window=20, min_periods=1).mean()
)
df["relative_volume"] = (df["volume"] / df["avg_volume_20d"]).round(2)

df.to_parquet("stocks_alpaca.parquet", compression="snappy", row_group_size=100_000)
