from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import os
import json

# Your Finnhub API Key (set it as an Airflow Variable or Env Var)
FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY", "your_finnhub_api_key")

# Output folder
OUTPUT_PATH = "/tmp/financial_statements"

def fetch_financials(**kwargs):
    """
    Fetch financial statements (income statement, balance sheet, cash flow)
    from Finnhub API and save as JSON.
    """
    symbol = kwargs['params'].get('symbol', 'AAPL')
    url = f"https://finnhub.io/api/v1/stock/financials-reported?symbol={symbol}&token={FINNHUB_API_KEY}"

    response = requests.get(url)
    if response.status_code != 200:
        raise Exception(f"Failed to fetch financials: {response.text}")

    data = response.json()

    os.makedirs(OUTPUT_PATH, exist_ok=True)
    file_path = os.path.join(OUTPUT_PATH, f"{symbol}_financials.json")

    with open(file_path, "w") as f:
        json.dump(data, f, indent=4)

    print(f"Saved financial statements for {symbol} → {file_path}")

# Default arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    "financial_statements_dag",
    default_args=default_args,
    description="Fetch company financial statements from Finnhub API",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["financial", "api"],
) as dag:

    fetch_financials_task = PythonOperator(
        task_id="fetch_financials",
        python_callable=fetch_financials,
        provide_context=True,
        params={"symbol": "AAPL"},  # Change to MSFT, TSLA, etc.
    )

    fetch_financials_task
