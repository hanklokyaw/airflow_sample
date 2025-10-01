from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
import requests
import pandas as pd
import os
import logging

# Config
API_KEY = "YOUR_FINNHUB_API_KEY"   # <-- replace with your real key
SYMBOLS = ["AAPL", "MSFT", "TSLA"]  # add more symbols here
OUTPUT_DIR = "/opt/airflow/data/finnhub"

BASE_URL = "https://finnhub.io/api/v1/stock/candle"


# Shared function: fetch + transform + save
def fetch_and_save(symbol):
    """Fetch stock data from Finnhub and save to CSV"""
    logging.info("Fetching stock data for %s", symbol)

    # Time range: last 30 days
    now = int(datetime.now().timestamp())
    thirty_days_ago = now - 30 * 24 * 60 * 60

    params = {
        "symbol": symbol,
        "resolution": "D",    # Daily candles
        "from": thirty_days_ago,
        "to": now,
        "token": API_KEY,
    }

    response = requests.get(BASE_URL, params=params)
    response.raise_for_status()
    data = response.json()

    if data.get("s") != "ok":
        raise ValueError(f"Unexpected API response for {symbol}: {data}")

    df = pd.DataFrame({
        "date": pd.to_datetime(data["t"], unit="s"),
        "open": data["o"],
        "high": data["h"],
        "low": data["l"],
        "close": data["c"],
        "volume": data["v"],
    })
    df["symbol"] = symbol
    df["processed_at"] = datetime.now()

    # Ensure output dir exists
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    out_file = os.path.join(OUTPUT_DIR, f"{symbol}_prices.csv")

    df.to_csv(out_file, index=False)
    logging.info("Saved %d rows for %s to %s", len(df), symbol, out_file)
    return out_file


# Define DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["alerts@example.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    "finnhub_multi_stock_pipeline",
    default_args=default_args,
    description="Fetch multiple stock prices from Finnhub API",
    schedule_interval="@daily",
    start_date=datetime(2025, 9, 25),
    catchup=False,
    tags=["stocks", "finnhub", "multi-ticker"],
) as dag:

    @task
    def extract_and_save(symbol: str):
        return fetch_and_save(symbol)

    # Dynamic task mapping for all symbols
    extract_and_save.expand(symbol=SYMBOLS)
