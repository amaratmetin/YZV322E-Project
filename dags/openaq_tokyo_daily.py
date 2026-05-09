from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="openaq_tokyo_daily",
    description="Download one day of archived OpenAQ Tokyo CSV data, transform it with Polars, and load PostgreSQL.",
    schedule="0 0 1-30 4 *",
    start_date=datetime(2026, 4, 1),
    end_date=datetime(2026, 5, 1),
    catchup=True,
    max_active_runs=1,
    tags=["openaq", "tokyo", "air-quality"],
) as dag:
    fetch_locations = BashOperator(
        task_id="fetch_locations",
        bash_command="cd /opt/airflow && python scripts/fetch_locations.py",
    )

    fetch_measurements = BashOperator(
        task_id="fetch_measurements",
        bash_command="cd /opt/airflow && TARGET_DATE={{ ds }} python scripts/fetch_measurements.py",
    )

    transform_load = BashOperator(
        task_id="transform_load",
        bash_command="cd /opt/airflow && TARGET_DATE={{ ds }} python scripts/transform_load.py",
    )

    fetch_locations >> fetch_measurements >> transform_load
