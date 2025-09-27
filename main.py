from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
import logging
import pandas as pd
import os

# Paths
INPUT_FILE = "/opt/airflow/data/input.csv"
OUTPUT_FILE = "/opt/airflow/data/transformed.csv"

# Python functions
def extract_data():
    logging.info("Extracting data from %s", INPUT_FILE)
    if not os.path.exists(INPUT_FILE):
        raise FileNotFoundError(f"{INPUT_FILE} not found!")
    df = pd.read_csv(INPUT_FILE)
    logging.info("Data extracted: %s rows", len(df))
    return df.to_json()  # Pass to next task via XCom

def transform_data(ti):
    df_json = ti.xcom_pull(task_ids="extract_task")
    df = pd.read_json(df_json)
    logging.info("Transforming data...")
    df["processed_at"] = datetime.now()
    df.to_csv(OUTPUT_FILE, index=False)
    logging.info("Data transformed and saved to %s", OUTPUT_FILE)

def load_data():
    logging.info("Loading data to database (simulated)")
    # Replace with real DB connection (PostgresOperator, Snowflake, etc.)
    logging.info("Data loaded successfully!")

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

# Define DAG
with DAG(
    "etl_monitoring_workflow",
    default_args=default_args,
    description="Example ETL workflow with monitoring",
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["etl", "monitoring"],
) as dag:

    # Task 1: Monitor file existence
    wait_for_file = FileSensor(
        task_id="wait_for_file",
        filepath=INPUT_FILE,
        poke_interval=30,  # check every 30s
        timeout=600,       # fail if not found in 10 min
        mode="poke"
    )

    # Task 2: Extract
    extract = PythonOperator(
        task_id="extract_task",
        python_callable=extract_data
    )

    # Task 3: Transform
    transform = PythonOperator(
        task_id="transform_task",
        python_callable=transform_data
    )

    # Task 4: Load
    load = PythonOperator(
        task_id="load_task",
        python_callable=load_data
    )

    # DAG dependencies
    wait_for_file >> extract >> transform >> load
