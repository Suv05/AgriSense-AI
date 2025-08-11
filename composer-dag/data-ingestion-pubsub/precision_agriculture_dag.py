from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pytz
import os
import sys

# === Import your ingestion scripts ===
# Assuming scripts are in dags/data-ingestion
DAGS_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(DAGS_DIR, "data-ingestion"))

from weather_ingest import fetch_weather_data
from earth_engine_ingest import remote_sensing_data, LOCATIONS

# === Define default arguments ===
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["alerts@example.com"],  # Change to your alert email
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# === Create DAG ===
with DAG(
    dag_id="precision_agriculture_ingestion",
    default_args=default_args,
    description="Ingests weather and Earth Engine data for precision agriculture",
    schedule_interval="30 19 * * *",  # 1:00 AM IST every day
    start_date=datetime(2025, 8, 11, tzinfo=pytz.UTC),
    catchup=False,
    tags=["precision-agriculture", "data-ingestion"],
) as dag:

    # Task 1: Weather data ingestion
    weather_task = PythonOperator(
        task_id="fetch_weather_data",
        python_callable=fetch_weather_data,
    )

    # Task 2: Earth Engine ingestion for all locations
    def run_earth_engine_ingestion():
        from google.cloud import pubsub_v1
        import json
        import logging

        PROJECT_ID = "careful-trainer-p1"
        TOPIC_ID = "earth-engine-data-pub"

        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

        for location in LOCATIONS:
            data = remote_sensing_data(location)
            if data:
                logging.info(f"Publishing data for {location['name']}")
                future = publisher.publish(
                    topic_path, json.dumps(data, default=str).encode("utf-8")
                )
                future.result()

    earth_engine_task = PythonOperator(
        task_id="fetch_earth_engine_data",
        python_callable=run_earth_engine_ingestion,
    )

    # Set task dependencies
    weather_task >> earth_engine_task
