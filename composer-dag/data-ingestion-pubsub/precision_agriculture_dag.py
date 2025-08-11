from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta

import pytz
from airflow import DAG
from airflow.operators.python import PythonOperator

# === Import your ingestion scripts ===
# Get the directory of the current DAG file and add the data-ingestion subdirectory
dag_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(dag_dir, "data-ingestion"))

# Import the functions. The scripts are now structured to be called directly.
from fetch_and_publish_earth_engine_data import ingest_earth_engine_data
from fetch_and_publish_weather_data import ingest_weather_data

# === Define default arguments ===
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["suvampravu@gmail.com"], # It's good practice to add your email here
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Define the timezone for the schedule
# IST is UTC+5:30. 1:00 AM IST is 19:30 UTC of the previous day.
IST_TZ = pytz.timezone("Asia/Kolkata")
utc_start_date = datetime(2025, 8, 11, tzinfo=pytz.UTC)

# === Create DAG ===
with DAG(
    dag_id="precision_agriculture_ingestion",
    default_args=default_args,
    description="Ingests weather and Earth Engine data for precision agriculture",
    schedule_interval="30 19 * * *",  # 1:00 AM IST (19:30 UTC) every day
    start_date=utc_start_date,
    catchup=False,
    tags=["precision-agriculture", "data-ingestion"],
) as dag:
    
    # Task 1: Weather data ingestion
    weather_task = PythonOperator(
        task_id="ingest_weather_data",
        python_callable=ingest_weather_data,
    )

    # Task 2: Earth Engine ingestion for all locations
    earth_engine_task = PythonOperator(
        task_id="ingest_earth_engine_data",
        python_callable=ingest_earth_engine_data,
        )

    # Set task dependencies
    # It's better to run these in parallel if they are independent.
    # The current code suggests they can run in parallel.
    weather_task
    earth_engine_task