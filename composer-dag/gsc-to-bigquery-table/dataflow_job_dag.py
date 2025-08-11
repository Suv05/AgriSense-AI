from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta

import pytz
from airflow import DAG
from airflow.operators.python import PythonOperator

# === Import your Dataflow job scripts ===
# Get the directory of the current DAG file
dag_dir = os.path.dirname(os.path.abspath(__file__))
# Add the dataflow-jobs subdirectory to the Python path
sys.path.append(os.path.join(dag_dir, "dataflow-jobs"))

# Import the functions that contain your Dataflow pipeline logic
from ingest_weather_dataflow_batch import ingest_weather_dataflow
from ingest_gee_dataflow_batch import ingest_gee_dataflow

# === Define default arguments for the DAG ===
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["suvampravu@gmail.com"],  # It's good practice to add your email here
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# === Configure the DAG schedule ===
# IST is UTC+5:30. A 2:00 AM IST run time translates to 20:30 UTC of the previous day.
utc_start_date = datetime(2025, 8, 11, tzinfo=pytz.UTC)

# === Create the DAG ===
with DAG(
    dag_id="dataflow_batch_jobs",
    default_args=default_args,
    description="Launches Dataflow batch jobs to ingest weather and GEE data into BigQuery.",
    # The cron expression '30 20 * * *' schedules the DAG to run at 20:30 UTC every day,
    schedule_interval="30 20 * * *",
    start_date=utc_start_date,
    catchup=False,
    tags=["dataflow", "batch", "bigquery"],
) as dag:
    
    # Task 1: Launch the Dataflow job for weather data
    weather_dataflow_job = PythonOperator(
        task_id="launch_weather_dataflow",
        python_callable=ingest_weather_dataflow,
    )

    # Task 2: Launch the Dataflow job for Earth Engine data
    gee_dataflow_job = PythonOperator(
        task_id="launch_gee_dataflow",
        python_callable=ingest_gee_dataflow,
    )

    # The two data ingestion pipelines are independent, so we run them in parallel.
    # This dependency line is not needed as they run independently by default.
    # [weather_dataflow_job, gee_dataflow_job]
