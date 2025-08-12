import requests
import json
import os
import time
from google.cloud import pubsub_v1
from google.cloud import storage


def ingest_weather_data():
    
    # --- Configuration ---
    # These environment variables are set in the Cloud Function's configuration.
    project_id = "careful-trainer-p1"
    topic_id = "weather-data-pub"
    api_key = os.environ.get("OPEN_WEATHER_API_KEY_1")

    if not all([project_id, topic_id, api_key]):
        print("Error: One or more environment variables are missing.")
        return
    
    # Load locations from the config.json file which must be deployed with the function.
    try:
        storage_client = storage.Client()
        bucket_name = "asia-south1-data-ingestion--de14e7ec-bucket" # Use your bucket name
        blob = storage_client.bucket(bucket_name).blob("dags/data-ingestion/config.json")
        LOCATIONS = json.loads(blob.download_as_string())
    except Exception as e:
        print(f"Error loading config from GCS: {e}")
        return

    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_id)
    
    # --- Core Logic ---
    
    # Fetch weather data for each location and publish to Pub/Sub
    for location in LOCATIONS:
        try:
            url = (
                f"https://api.openweathermap.org/data/2.5/weather"
                f"?lat={location['lat']}&lon={location['lon']}&appid={api_key}&units=metric"
            )

            response = requests.get(url)
            response.raise_for_status()  # Raise an error for bad responses
            
            weather_data = response.json()
            weather_data["location"] = location["name"]  # Add location name to data
            
            # publish weather data to Pub/Sub in dictionary format
            json_data = json.dumps(weather_data, default=str)
            data_bytes = json_data.encode("utf-8")
            
            # Publish the data to the Pub/Sub topic
            future = publisher.publish(topic_path, data_bytes)
            print(f"Published message ID: {future.result()} for location: {location['name']}")
            
            time.sleep(1)  # Sleep to avoid hitting API rate limits
            
        except requests.RequestException as e:
            print(f"Error fetching weather data for {location['name']}: {e}")
        except Exception as e:
            print(f"An unexpected error occurred for {location['name']}: {e}")

    print("Weather data ingestion completed.")