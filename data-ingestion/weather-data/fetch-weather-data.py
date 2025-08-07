import requests
import json
import os
import time
from google.cloud import pubsub_v1
from dotenv import load_dotenv

project_id = "careful-trainer-p1"
topic_id = "weather-data-pub"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)

# Load environment variables
load_dotenv()
api_key = os.getenv("OPEN_WEATHER_API_KEY_1")

# Load config from file (once)
with open("./data-ingestion/google-earth-engine-data/config.json", "r") as f:
    LOCATIONS = json.load(f)


# publish weather data to Pub/Sub in dictionary format
def publish_weather_data(weather_data):
    try:
        json_data = json.dumps(weather_data, default=str)
        data_bytes = json_data.encode("utf-8")
        # Publish the data to the Pub/Sub topic
        future = publisher.publish(topic_path, data_bytes)
        print(f"Published message ID: {future.result()}")
    except Exception as e:
        print(f"Error publishing message: {e}")


# Fetch weather data for each location in config
def fetch_weather_data():
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
            print(f"Fetched weather data for {location}: {weather_data}")
            publish_weather_data(weather_data)
            time.sleep(2)  # Sleep to avoid hitting API rate limits
        except requests.RequestException as e:
            print(f"Error fetching weather data for {location}: {e}")
    
if __name__ == "__main__":
    print("Starting weather data ingestion...")
    fetch_weather_data()
    print("Weather data ingestion completed.")
