import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions
import json
import datetime

class ParseEarthEngineJson(beam.DoFn):
    def process(self, element):
        try:
            data = json.loads(element)

            return [{
                'location': data.get('location'),
                'lat': data.get('lat'),
                'lon': data.get('lon'),
                'ndvi': data.get('ndvi'),
                'evi': data.get('evi'),
                'soil_moisture': data.get('soil_moisture'),
                'temperature_C': data.get('temperature_C'),
                'relative_humidity': data.get('relative_humidity'),
                'timestamp': data.get('timestamp')
            }]
        except Exception as e:
            print(f"Error parsing record: {e}")
            return []

def run():
    project_id = "careful-trainer-p1"
    region = "asia-south1"
    bucket_path = "gs://careful-trainer-p1-agri-datalake/raw/earth-engine/*.jsonl"
    dataset_id = "precision_dataset"
    table_id = "satelite_data"

    options = PipelineOptions()
    google_cloud_options = options.view_as(GoogleCloudOptions)
    google_cloud_options.project = project_id
    google_cloud_options.region = region
    google_cloud_options.temp_location = f"gs://{project_id}-dataflow-temp/temp"
    google_cloud_options.staging_location = f"gs://{project_id}-dataflow-temp/staging"
    options.view_as(StandardOptions).runner = "DataflowRunner"

    with beam.Pipeline(options=options) as p:
        (
            p
            | "Read JSON Lines from GCS" >> beam.io.ReadFromText(bucket_path)
            | "Parse Earth Engine JSON" >> beam.ParDo(ParseEarthEngineJson())
            | "Write to BigQuery" >> beam.io.WriteToBigQuery(
                table=f"{project_id}:{dataset_id}.{table_id}",
                schema={
                    "fields": [
                        {"name": "location", "type": "STRING", "mode": "REQUIRED"},
                        {"name": "lat", "type": "FLOAT", "mode": "NULLABLE"},
                        {"name": "lon", "type": "FLOAT", "mode": "NULLABLE"},
                        {"name": "ndvi", "type": "FLOAT", "mode": "NULLABLE"},
                        {"name": "evi", "type": "FLOAT", "mode": "NULLABLE"},
                        {"name": "soil_moisture", "type": "FLOAT", "mode": "NULLABLE"},
                        {"name": "temperature_C", "type": "FLOAT", "mode": "NULLABLE"},
                        {"name": "relative_humidity", "type": "FLOAT", "mode": "NULLABLE"},
                        {"name": "timestamp", "type": "TIMESTAMP", "mode": "NULLABLE"}
                    ]
                },
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
            )
        )

if __name__ == "__main__":
    run()

#  python raw-gee-bigquery.py --runner=DataflowRunner --project=careful-trainer-p1 --region=asia-south1 --worker_zone=asia-south1-b --worker_machine_type=e2-small --num_workers=1 --max_num_workers=3 --temp_location=gs://careful-trainer-p1-dataflow-temp/temp --staging_location=gs://careful-trainer-p1-dataflow-temp/staging --job_name=ingest-earth-engine-bigquery-v0 --save_main_session
