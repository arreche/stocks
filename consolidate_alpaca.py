import duckdb

duckdb.sql("""
COPY (
  SELECT 
    regexp_replace(filename, '^.*/|\\.parquet$', '', 'g') AS symbol,
    t.*
  FROM read_parquet('alpaca_data/*.parquet') t
)
TO 'stock_prices_alpaca.parquet' (FORMAT PARQUET);
""")
