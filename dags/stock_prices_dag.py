from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging
import pandas as pd
import requests
import os

# Config
API_KEY = "YOUR_ALPHA_VANTAGE_API_KEY"   # get from https://www.alphavantage.co/support/#api-key
SYMBOL = "AAPL"
OUTPUT_FILE = "/opt/airflow/data/stock_prices.csv"
BASE_URL = "https://www.alphavantage.co/query"

# Python functions
def extract_data():
    """Fetch stock data from Alpha Vantage API"""
    logging.info("Fetching stock data for %s", SYMBOL)
    params = {
        "function": "TIME_SERIES_DAILY_ADJUSTED",
        "symbol": SYMBOL,
        "apikey": API_KEY,
    }
    response = requests.get(BASE_URL, params=params)
    response.raise_for_status()
    data = response.json()

    if "Time Series (Daily)" not in data:
        raise ValueError(f"Unexpected API response: {data}")

    ts_data = data["Time Series (Daily)"]
    df = pd.DataFrame.from_dict(ts_data, orient="index")
    df.index = pd.to_datetime(df.index)
    df = df.sort_index().reset_index()
    df.rename(columns={"index": "date"}, inplace=True)
    df["symbol"] = SYMBOL

    logging.info("Extracted %d rows for %s", len(df), SYMBOL)
    return df.to_json()  # send via XCom


def transform_data(ti):
    """Add processing timestamp and clean columns"""
    df_json = ti.xcom_pull(task_ids="extract_task")
    df = pd.read_json(df_json)

    logging.info("Transforming data...")
    df["processed_at"] = datetime.now()

    # Save locally
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    df.to_csv(OUTPUT_FILE, index=False)
    logging.info("Data saved to %s", OUTPUT_FILE)


def load_data():
    """Simulate loading data into a database"""
    logging.info("Loading data to database (simulated)")
    # Example: Use PostgresOperator in real case
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
    "stock_prices_pipeline",
    default_args=default_args,
    description="ETL workflow for stock prices via API",
    schedule_interval="@daily",   # run once per day
    start_date=datetime(2025, 9, 25),  # must be in the past
    catchup=False,
    tags=["etl", "stocks"],
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
