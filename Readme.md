# Stock Prices Pipeline with Airflow

This project demonstrates how to use Apache Airflow to orchestrate a simple ETL workflow that fetches stock prices from the Alpha Vantage API, transforms the data, and simulates loading it into a database.

## Requirements
- Python 3.8+
- Apache Airflow 2.x
- Dependencies: requests, pandas

Install dependencies:
```
pip install apache-airflow requests pandas
```

## Setup Instructions
1. **Alpha Vantage API Key**
   - Get a free API key from: https://www.alphavantage.co/support/#api-key
   - Replace `YOUR_ALPHA_VANTAGE_API_KEY` in `stock_prices_dag.py`.

2. **Airflow Setup**
   - Place `stock_prices_dag.py` in your Airflow `dags/` folder.
   - Ensure the data directory exists:
     ```
     mkdir -p /opt/airflow/data
     ```

3. **Run Airflow**
   - Initialize Airflow:
     ```
     airflow db init
     ```
   - Start scheduler and webserver:
     ```
     airflow scheduler &
     airflow webserver -p 8080 &
     ```
   - Access Airflow UI at: http://localhost:8080

4. **Trigger the DAG**
   - In the Airflow UI, enable and trigger the DAG named `stock_prices_pipeline`.
   - The DAG will:
     - Fetch stock prices (default: AAPL).
     - Transform and add `processed_at` column.
     - Save results to `/opt/airflow/data/stock_prices.csv`.
     - Simulate loading data to a database.

## Customization
- Change the stock symbol by editing the `SYMBOL` variable in `stock_prices_dag.py`.
- To add more tickers, extend the DAG with multiple tasks or use dynamic task mapping.
- Replace the `load_data()` function with a real database operator (`PostgresOperator`, `MySqlOperator`, etc.).

## Notes
- The DAG is scheduled to run daily (`@daily`).
- Logs and retries are handled by Airflow.
- Email alerts can be configured by editing `default_args`.

---

Happy orchestrating with Airflow!
