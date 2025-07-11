import duckdb

duckdb.sql("""
COPY (
  SELECT 
    regexp_replace(filename, '^.*/|\\.parquet$', '', 'g') AS Symbol,
    t.*
  FROM read_parquet('parquet_data/*.parquet') t
)
TO 'stock_prices.parquet' (FORMAT PARQUET);
""")

