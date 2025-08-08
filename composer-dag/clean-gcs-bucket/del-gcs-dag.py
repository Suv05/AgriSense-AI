from airflow import DAG
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator
from datetime import datetime

with DAG(
    dag_id="daily_gcs_cleanup",
    start_date=datetime(2025, 1, 1),
    schedule_interval="30 17 * * *",
    catchup=False,
    tags=['gcs', 'cleanup'],
) as dag:

    delete_earth_engine_data = GCSDeleteObjectsOperator(
        task_id="delete_earth_engine_data",
        bucket_name="careful-trainer-p1-agri-datalake",
        prefix="raw/earth-engine/",
        objects=None,
    )

    delete_weather_data = GCSDeleteObjectsOperator(
        task_id="delete_weather_data",
        bucket_name="careful-trainer-p1-agri-datalake",
        prefix="raw/weather/",
        objects=None,
    )

    delete_earth_engine_data >> delete_weather_data
