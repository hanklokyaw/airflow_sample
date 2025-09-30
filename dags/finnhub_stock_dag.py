from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging
import pandas as pd
import requests
import os

# Config
API_KEY = "YOUR_FINNHUB_API_KEY"   # <-- replace with your real key
SYMBOL = "AAPL"
OUTPUT_FILE = "/opt/airflow/data/finnhub_stock_prices.csv"
BASE_URL = "https://finnhub.io/api/v1/stock/candle"

# Python functions
def extract_data():
    """Fetch stock candle data from Finnhub API"""
    logging.info("Fetching stock data for %s", SYMBOL)

    # Time range: last 30 days
    now = int(datetime.now().timestamp())
    thirty_days_ago = now - 30 * 24 * 60 * 60

    params = {
        "symbol": SYMBOL,
        "resolution": "D",    # Daily candles
        "from": thirty_days_ago,
        "to": now,
        "token": API_KEY,
    }

    response = requests.get(BASE_URL, params=params)
    response.raise_for_status()
    data = response.json()

    if data.get("s") != "ok":
        raise ValueError(f"Unexpected API response: {data}")

    # Convert to DataFrame
    df = pd.DataFrame({
        "date": pd.to_datetime(data["t"], unit="s"),
        "open": data["o"],
        "high": data["h"],
        "low": data["l"],
        "close": data["c"],
        "volume": data["v"],
    })
    df["symbol"] = SYMBOL

    logging.info("Extracted %d rows for %s", len(df), SYMBOL)
    return df.to_json()


def transform_data(ti):
    """Add processing timestamp and save as CSV"""
    df_json = ti.xcom_pull(task_ids="extract_task")
    df = pd.read_json(df_json)

    logging.info("Transforming data...")
    df["processed_at"] = datetime.now()

    # Save to CSV
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    df.to_csv(OUTPUT_FILE, index=False)
    logging.info("Data saved to %s", OUTPUT_FILE)


def load_data():
    """Simulate loading into DB"""
    logging.info("Loading data into database (simulated)")
    logging.info("Data load successful!")


# Default args
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["alerts@example.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

# DAG definition
with DAG(
    "finnhub_stock_pipeline",
    default_args=default_args,
    description="Fetch stock prices from Finnhub API",
    schedule_interval="@daily",
    start_date=datetime(2025, 9, 25),
    catchup=False,
    tags=["stocks", "finnhub"],
) as dag:

    extract = PythonOperator(
        task_id="extract_task",
        python_callable=extract_data,
    )

    transform = PythonOperator(
        task_id="transform_task",
        python_callable=transform_data,
    )

    load = PythonOperator(
        task_id="load_task",
        python_callable=load_data,
    )

    extract >> transform >> load
