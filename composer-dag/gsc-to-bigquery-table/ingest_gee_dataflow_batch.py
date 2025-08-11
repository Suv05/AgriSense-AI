import apache_beam as beam
from apache_beam.options.pipeline_options import (
    PipelineOptions,
    GoogleCloudOptions,
    StandardOptions,
)
import json
import datetime


def ingest_gee_dataflow():
    # --- Dataflow Pipeline Logic ---

    # This function is used to parse and flatten the JSON data from GCS.
    class ParseEarthEngineJson(beam.DoFn):
        def process(self, element):
            try:
                data = json.loads(element)
                # Adding a check to ensure 'dt' exists for the timestamp
                timestamp = data.get("timestamp")
                if not timestamp:
                    # If timestamp is missing, use current time as a fallback
                    timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat()

                return [
                    {
                        "location": data.get("location"),
                        "lat": data.get("lat"),
                        "lon": data.get("lon"),
                        "ndvi": data.get("ndvi"),
                        "evi": data.get("evi"),
                        "soil_moisture": data.get("soil_moisture"),
                        "temperature_C": data.get("temperature_C"),
                        "relative_humidity": data.get("relative_humidity"),
                        "timestamp": timestamp,
                    }
                ]
            except Exception as e:
                print(f"Error parsing record: {e}")
                return []

    # This function encapsulates the entire Dataflow pipeline definition.
    def run_dataflow_pipeline():
        project_id = "careful-trainer-p1"
        region = "asia-south1"
        bucket_path = f"gs://{project_id}-agri-datalake/raw/earth-engine/*.jsonl"
        dataset_id = "precision_dataset"
        table_id = "satelite_data"

        # Configure pipeline options
        options = PipelineOptions()
        google_cloud_options = options.view_as(GoogleCloudOptions)
        google_cloud_options.project = project_id
        google_cloud_options.region = region
        google_cloud_options.job_name = (
            f"gee-ingestion-job-{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}"
        )
        google_cloud_options.staging_location = (
            f"gs://{project_id}-dataflow-temp/staging"
        )
        google_cloud_options.temp_location = f"gs://{project_id}-dataflow-temp/temp"

        options.view_as(StandardOptions).runner = "DataflowRunner"

        table_spec = f"{project_id}:{dataset_id}.{table_id}"

        table_schema = {
            "fields": [
                {"name": "location", "type": "STRING", "mode": "REQUIRED"},
                {"name": "lat", "type": "FLOAT", "mode": "NULLABLE"},
                {"name": "lon", "type": "FLOAT", "mode": "NULLABLE"},
                {"name": "ndvi", "type": "FLOAT", "mode": "NULLABLE"},
                {"name": "evi", "type": "FLOAT", "mode": "NULLABLE"},
                {"name": "soil_moisture", "type": "FLOAT", "mode": "NULLABLE"},
                {"name": "temperature_C", "type": "FLOAT", "mode": "NULLABLE"},
                {"name": "relative_humidity", "type": "FLOAT", "mode": "NULLABLE"},
                {"name": "timestamp", "type": "TIMESTAMP", "mode": "NULLABLE"},
            ]
        }

        # Build and run the pipeline
        with beam.Pipeline(options=options) as p:
            (
                p
                | "Read JSON Lines from GCS" >> beam.io.ReadFromText(bucket_path)
                | "Parse Earth Engine JSON" >> beam.ParDo(ParseEarthEngineJson())
                | "Write to BigQuery"
                >> beam.io.WriteToBigQuery(
                    table_spec,
                    schema=table_schema,
                    # This line was changed to overwrite the table on each run
                    write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                    custom_gcs_temp_location=f"gs://{project_id}-dataflow-temp/temp",
                )
            )

        print("Dataflow pipeline launched successfully.")

    # Call the function to run the pipeline
    run_dataflow_pipeline()
