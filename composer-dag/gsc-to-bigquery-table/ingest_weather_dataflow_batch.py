import os
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions, WorkerOptions
import json
import datetime


def ingest_weather_dataflow():

    # --- Dataflow Pipeline Logic ---
    
    # This function is used to parse and flatten the JSON data from GCS.
    def parse_and_flatten_json(record):
        try:
            data = json.loads(record)

            return {
                "lon": data.get("coord", {}).get("lon"),
                "lat": data.get("coord", {}).get("lat"),
                "temp": data.get("main", {}).get("temp"),
                "pressure": data.get("main", {}).get("pressure"),
                "humidity": data.get("main", {}).get("humidity"),
                "sea_level": data.get("main", {}).get("sea_level"),
                "grnd_level": data.get("main", {}).get("grnd_level"),
                "wind_speed": data.get("wind", {}).get("speed"),
                "wind_deg": data.get("wind", {}).get("deg"),
                "wind_gust": data.get("wind", {}).get("gust"),
                "weather_main": data.get("weather", [{}])[0].get("main"),
                "weather_description": data.get("weather", [{}])[0].get("description"),
                "country": data.get("sys", {}).get("country"),
                "location": data.get("location"),
                "timestamp": datetime.datetime.utcfromtimestamp(data.get("dt")).isoformat() if data.get("dt") else None
            }
        except Exception as e:
            print(f"Error parsing record: {e}")
            return None

    # This function encapsulates the entire Dataflow pipeline definition.
    def run_dataflow_pipeline():
        project_id = "careful-trainer-p1"
        region = "asia-south1"
        bucket_path = f"gs://{project_id}-agri-datalake/raw/weather/*.jsonl"
        dataset_id = "precision_dataset"
        table_id = "weather_data"

        # Configure pipeline options with cost-effective settings
        options = PipelineOptions(
            setup_file=os.path.join(os.path.dirname(__file__), "..", "setup.py")
        )
        
        # Google Cloud options
        google_cloud_options = options.view_as(GoogleCloudOptions)
        google_cloud_options.project = project_id
        google_cloud_options.region = region
        google_cloud_options.job_name = "ingest-weather-data-bigquery-v2"
        google_cloud_options.staging_location = f"gs://{project_id}-dataflow-temp/staging"
        google_cloud_options.temp_location = f"gs://{project_id}-dataflow-temp/temp"

        # Standard options
        standard_options = options.view_as(StandardOptions)
        standard_options.runner = "DataflowRunner"

        # Worker options for cost optimization
        worker_options = options.view_as(WorkerOptions)
        worker_options.machine_type = "e2-small"
        worker_options.num_workers = 1
        worker_options.max_num_workers = 3
        worker_options.zone = "asia-south1-b"

        table_spec = f"{project_id}:{dataset_id}.{table_id}"

        table_schema = {
            "fields": [
                {"name": "lon", "type": "FLOAT", "mode": "REQUIRED"},
                {"name": "lat", "type": "FLOAT", "mode": "REQUIRED"},
                {"name": "temp", "type": "FLOAT"},
                {"name": "pressure", "type": "INTEGER"},
                {"name": "humidity", "type": "INTEGER"},
                {"name": "sea_level", "type": "INTEGER"},
                {"name": "grnd_level", "type": "INTEGER"},
                {"name": "wind_speed", "type": "FLOAT"},
                {"name": "wind_deg", "type": "INTEGER"},
                {"name": "wind_gust", "type": "FLOAT"},
                {"name": "weather_main", "type": "STRING"},
                {"name": "weather_description", "type": "STRING"},
                {"name": "country", "type": "STRING"},
                {"name": "location", "type": "STRING", "mode": "REQUIRED"},
                {"name": "timestamp", "type": "TIMESTAMP"}
            ]
        }

        # Build and run the pipeline
        with beam.Pipeline(options=options) as p:
            (
                p
                | "Read JSONL from GCS" >> beam.io.ReadFromText(bucket_path)
                | "Parse & Flatten" >> beam.Map(parse_and_flatten_json)
                | "Filter Non-None" >> beam.Filter(lambda x: x is not None)
                | "Write to BigQuery" >> beam.io.WriteToBigQuery(
                    table_spec,
                    schema=table_schema,
                    # This line was changed to overwrite the table on each run
                    write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                    custom_gcs_temp_location=f"gs://{project_id}-dataflow-temp/temp"
                )
            )

        print("Dataflow pipeline launched successfully.")
    
    # Call the function to run the pipeline
    run_dataflow_pipeline()