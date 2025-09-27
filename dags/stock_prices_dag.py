from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from stock_api import fetch_stock_data

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["alerts@example.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "stock_prices_pipeline",
    default_args=default_args,
    description="Fetch stock prices daily via API",
    schedule_interval="@daily",   # runs once per day
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["stocks", "api"],
) as dag:

    fetch_prices = PythonOperator(
        task_id="fetch_stock_prices",
        python_callable=fetch_stock_data,
        op_kwargs={"symbol": "AAPL"},  # you can change to MSFT, TSLA, etc.
    )
