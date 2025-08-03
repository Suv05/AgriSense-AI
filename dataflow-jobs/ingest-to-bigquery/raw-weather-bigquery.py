import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions
import json
import datetime

def parse_and_flatten_json(record):
    try:
        data = json.loads(record)

        return {
            "lon": data.get("coord", {}).get("lon"),
            "lat": data.get("coord", {}).get("lat"),
            "temp": data.get("main", {}).get("temp"),
            "temp_min": data.get("main", {}).get("temp_min"),
            "temp_max": data.get("main", {}).get("temp_max"),
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

def run():
    project_id = "careful-trainer-p1"
    region = "asia-south1"
    bucket_path = "gs://careful-trainer-p1-agri-datalake/raw/weather/*.jsonl"
    dataset_id = "precision_dataset"
    table_id = "weather_data"

    options = PipelineOptions()
    google_cloud_options = options.view_as(GoogleCloudOptions)
    google_cloud_options.project = project_id
    google_cloud_options.region = region
    google_cloud_options.job_name = "weather-gcs-to-bq"
    google_cloud_options.staging_location = f"gs://{project_id}-dataflow-temp/staging"
    google_cloud_options.temp_location = f"gs://{project_id}-dataflow-temp/temp"

    options.view_as(StandardOptions).runner = "DataflowRunner"

    table_spec = f"{project_id}:{dataset_id}.{table_id}"

    table_schema = {
        "fields": [
            {"name": "lon", "type": "FLOAT", "mode": "REQUIRED"},
            {"name": "lat", "type": "FLOAT", "mode": "REQUIRED"},
            {"name": "temp", "type": "FLOAT"},
            {"name": "temp_min", "type": "FLOAT"},
            {"name": "temp_max", "type": "FLOAT"},
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
            {"name": "location", "type": "STRING"},
            {"name": "timestamp", "type": "TIMESTAMP"}
        ]
    }

    with beam.Pipeline(options=options) as p:
        (
            p
            | "Read JSONL from GCS" >> beam.io.ReadFromText(bucket_path)
            | "Parse & Flatten" >> beam.Map(parse_and_flatten_json)
            | "Filter Non-None" >> beam.Filter(lambda x: x is not None)
            | "Write to BigQuery" >> beam.io.WriteToBigQuery(
                table_spec,
                schema=table_schema,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                custom_gcs_temp_location=f"gs://{project_id}-dataflow-temp/temp"
            )
        )

if __name__ == "__main__":
    run()

# python raw-weather-bigquery.py --runner=DataflowRunner --project=careful-trainer-p1 --region=asia-south1 --worker_zone=asia-south1-b --worker_machine_type=e2-small --num_workers=1 --max_num_workers=3 --temp_location=gs://careful-trainer-p1-dataflow-temp/temp --staging_location=gs://careful-trainer-p1-dataflow-temp/staging --job_name=ingest-weather-data-bigquery-v1 --save_main_session