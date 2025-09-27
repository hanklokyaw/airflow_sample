import requests
import pandas as pd
import logging
from datetime import datetime

API_KEY = "YOUR_ALPHA_VANTAGE_API_KEY"  # <-- get from https://www.alphavantage.co/support/#api-key
BASE_URL = "https://www.alphavantage.co/query"
OUTPUT_FILE = "/opt/airflow/data/stock_prices.csv"

def fetch_stock_data(symbol="AAPL"):
    """Fetch daily stock prices from Alpha Vantage"""
    logging.info("Fetching stock data for %s", symbol)
    params = {
        "function": "TIME_SERIES_DAILY_ADJUSTED",
        "symbol": symbol,
        "apikey": API_KEY,
        "datatype": "json"
    }
    response = requests.get(BASE_URL, params=params)
    response.raise_for_status()
    data = response.json()

    if "Time Series (Daily)" not in data:
        raise ValueError(f"Unexpected response: {data}")

    ts_data = data["Time Series (Daily)"]
    df = pd.DataFrame.from_dict(ts_data, orient="index")
    df.index = pd.to_datetime(df.index)
    df = df.sort_index().reset_index()
    df.rename(columns={"index": "date"}, inplace=True)
    df["symbol"] = symbol

    # Save to CSV
    df.to_csv(OUTPUT_FILE, index=False)
    logging.info("Saved %d rows to %s", len(df), OUTPUT_FILE)

    return OUTPUT_FILE
