# stocks

This project is a Python script that downloads and stores historical
stock market data. It fetches a list of stock symbols from NASDAQ, then
uses the yfinance library to download the historical data for each
symbol and saves it locally in Parquet format. It's designed to
efficiently update the data, only downloading new information for stocks
that haven't been updated recently.
