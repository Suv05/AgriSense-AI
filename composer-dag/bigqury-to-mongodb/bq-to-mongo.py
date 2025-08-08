from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from google.cloud import bigquery
from pymongo import MongoClient
import pytz
import os

# === CONFIGURATION ===
MONGODB_URI = os.environ.get("MONGODB_URI")
if not MONGODB_URI:
    raise ValueError("MONGODB_URI environment variable is not set.")


BIGQUERY_TABLE = "careful-trainer-p1.precision_dataset.final_table"
MONGO_DB = "precision_agri"
MONGO_COLLECTION = "final_data"

# === TASK FUNCTION ===
def bq_to_mongo(**kwargs):
    # BigQuery client
    bq_client = bigquery.Client()

    # Query the data
    query = f"SELECT * FROM `{BIGQUERY_TABLE}`"
    df = bq_client.query(query).to_dataframe()

    # Convert ARRAY<STRUCT<>> column to Python list of dicts
    if 'predicted_irrigation_need_probs' in df.columns:
        df['predicted_irrigation_need_probs'] = df['predicted_irrigation_need_probs'].apply(
            lambda x: [dict(label=val['label'], prob=val['prob']) for val in x]
        )

    # Add ingestion timestamp (IST timezone)
    ist = pytz.timezone('Asia/Kolkata')
    ingestion_time = datetime.now(ist).isoformat()
    df['ingestion_time'] = ingestion_time

    # Connect to MongoDB
    mongo_client = MongoClient(MONGODB_URI)
    db = mongo_client[MONGO_DB]
    collection = db[MONGO_COLLECTION]

    # Convert DataFrame to records
    records = df.to_dict("records")

    if records:
        # Optional: Remove any _id field to avoid ObjectId issues
        for record in records:
            record.pop("_id", None)
        collection.insert_many(records)
        print(f"{len(records)} records inserted.")
    else:
        print("No records found to insert.")


# === DAG DEFINITION ===
with DAG(
    dag_id="bq_to_mongodb_daily",
    start_date=datetime(2025, 1, 1),
    schedule_interval="30 0 * * *",  # 12:30 AM UTC = 6:00 AM IST
    catchup=False,
    tags=["bigquery", "mongodb", "daily-sync"],
) as dag:

    transfer_task = PythonOperator(
        task_id="transfer_bq_to_mongo",
        python_callable=bq_to_mongo,
    )
