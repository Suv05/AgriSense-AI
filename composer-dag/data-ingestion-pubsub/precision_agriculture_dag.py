from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta

import pytz
from airflow import DAG
from airflow.operators.python import PythonOperator

# === Add parent directory to path to allow importing scripts ===
# This is a robust way to ensure the scripts are importable.
# Get the directory of the current DAG file (dags/precision_agriculture_dag.py)
dag_dir = os.path.dirname(os.path.abspath(__file__))
# Add the parent directory (dags) and the data-ingestion subdirectory
sys.path.append(os.path.join(dag_dir, "data-ingestion"))

# Now import your functions directly. Airflow will handle the execution.
from earth_engine_ingest import fetch_and_publish_earth_engine_data
from weather_ingest import fetch_and_publish_weather_data

# === Define default arguments ===
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["suvampravu@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Define the timezone for the schedule
# IST is UTC+5:30. 1:00 AM IST is 19:30 UTC the previous day.
IST_TZ = pytz.timezone("Asia/Kolkata")

# === Create DAG ===
with DAG(
    dag_id="precision_agriculture_ingestion",
    default_args=default_args,
    description="Ingests weather and Earth Engine data for precision agriculture",
    schedule_interval="30 19 * * *",  # 1:00 AM IST (19:30 UTC) every day
    start_date=datetime(2025, 8, 11, tzinfo=pytz.UTC),
    catchup=False,
    tags=["precision-agriculture", "data-ingestion"],
) as dag:
    # Task 1: Weather data ingestion
    # You can call the function directly as it contains the loop
    weather_task = PythonOperator(
        task_id="fetch_weather_data",
        python_callable=fetch_and_publish_weather_data,
    )

    # Task 2: Earth Engine ingestion
    # You need to pass the callable to the PythonOperator.
    earth_engine_task = PythonOperator(
        task_id="fetch_earth_engine_data",
        python_callable=fetch_and_publish_earth_engine_data,
    )

    # Set task dependencies
    weather_task >> earth_engine_task