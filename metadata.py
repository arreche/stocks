import duckdb
import pandas as pd

url = "stock_prices.parquet"

query = f"""
SELECT
  Symbol AS symbol,
  Date AS date,
  Open AS open,
  Close AS close,
  High AS high,
  Low AS low,
  Volume AS volume
FROM read_parquet('{url}')
"""
df = duckdb.query(query).to_df()
df = df.sort_values(['symbol', 'date'])

df['prev_close'] = df.groupby('symbol')['close'].shift(1)
df['change_pct'] = ((df['close'] - df['prev_close']) / df['prev_close'] * 100).round(2)
df['gap_pct'] = ((df['open'] - df['prev_close']) / df['prev_close'] * 100).round(2)
df['run_pct'] = ((df['close'] - df['open']) / df['open'] * 100).round(2)
df['avg_volume_20d'] = df.groupby('symbol')['volume'].transform(lambda x: x.rolling(window=20, min_periods=1).mean())
df['relative_volume'] = (df['volume'] / df['avg_volume_20d']).round(2)

df.to_parquet('stocks.parquet', compression='snappy')
