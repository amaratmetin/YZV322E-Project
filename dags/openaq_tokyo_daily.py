from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="openaq_tokyo_daily",
    description="Extract raw OpenAQ Tokyo data, transform it with Polars, and load PostgreSQL.",
    schedule="@daily",
    start_date=datetime(2026, 5, 1),
    catchup=False,
    tags=["openaq", "tokyo", "air-quality"],
) as dag:
    fetch_locations = BashOperator(
        task_id="fetch_locations",
        bash_command="cd /opt/airflow && python scripts/fetch_locations.py",
    )

    fetch_sensors = BashOperator(
        task_id="fetch_sensors",
        bash_command="cd /opt/airflow && python scripts/fetch_sensors.py",
    )

    fetch_measurements = BashOperator(
        task_id="fetch_measurements",
        bash_command="cd /opt/airflow && python scripts/fetch_measurements.py",
    )

    transform_load = BashOperator(
        task_id="transform_load",
        bash_command="cd /opt/airflow && python scripts/transform_load.py",
    )

    fetch_locations >> fetch_sensors >> fetch_measurements >> transform_load
