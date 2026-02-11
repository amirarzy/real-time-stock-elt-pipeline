from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


def run_task():
    # Import inside task to avoid parse-time issues
    from elt.collector.config import load_settings
    from elt.collector.utils.logger import get_logger
    from elt.collector.job import run_once

    s = load_settings()
    logger = get_logger("airflow.market-data-collector", s.log_path)
    run_once(logger)


default_args = {
    "owner": "market_data",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=3),
}

with DAG(
    dag_id="market_data_yahoo_minutely",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule="* * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["market_data", "yahoo"],
) as dag:
    PythonOperator(
        task_id="run_yahoo_collector_once",
        python_callable=run_task,
        pool="yahoo_ingest",
    )
