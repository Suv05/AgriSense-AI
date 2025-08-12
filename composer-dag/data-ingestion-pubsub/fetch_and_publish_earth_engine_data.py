import json
import datetime
from google.cloud import pubsub_v1
import ee
from google.cloud import storage

def ingest_earth_engine_data():

    # --- Configuration ---
    # These environment variables are set in the Cloud Function's configuration.
    project_id = "careful-trainer-p1"
    topic_id = "earth-engine-data-pub"

    if not all([project_id, topic_id]):
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

    # Authenticate and Initialize Earth Engine
    # Initialize Earth Engine with your project ID
    try:
        ee.Initialize(project=project_id)
        print("Earth Engine initialized successfully.")
    except Exception as e:
        print(f"Error initializing Earth Engine: {e}")
        return

    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_id)

    # --- Core Logic Functions ---
    def mask_s2_clouds(image):
        """Masks clouds in a Sentinel-2 image using the QA band."""
        qa = image.select("QA60")
        cloud_bit_mask = 1 << 10
        cirrus_bit_mask = 1 << 11
        mask = qa.bitwiseAnd(cloud_bit_mask).eq(0).And(qa.bitwiseAnd(cirrus_bit_mask).eq(0))
        return image.updateMask(mask).divide(10000)

    def remote_sensing_data(location):
        """Fetches remote sensing data for a given region of interest (ROI)."""
        roi = ee.Geometry.Point([location["lon"], location["lat"]])

        try:
            # Sentinel-2 data
            sentinel = (
                ee.ImageCollection("COPERNICUS/S2_HARMONIZED")
                .filterDate("2025-06-05", "2025-07-05")
                .filterBounds(roi)
                .filter(ee.Filter.lt("CLOUDY_PIXEL_PERCENTAGE", 20))
                .map(mask_s2_clouds)
                .select(["B2", "B4", "B8"])
                .median()
            )
            
            # Check if sentinel image collection is empty
            if sentinel.bandNames().size().getInfo() == 0:
                print(f"No Sentinel-2 data found for {location['name']} in the specified date range.")
                return None

            # Calculate NDVI and EVI
            ndvi = sentinel.normalizedDifference(["B8", "B4"]).rename("NDVI")
            evi = sentinel.expression(
                "2.5 * ((NIR - RED) / (NIR + 6 * RED - 7.5 * BLUE + 1))",
                {"NIR": sentinel.select("B8"), "RED": sentinel.select("B4"), "BLUE": sentinel.select("B2")},
            ).rename("EVI")

            # ERA5-Land data
            era5 = (
                ee.ImageCollection("ECMWF/ERA5_LAND/HOURLY")
                .filterDate("2025-06-01", "2025-07-31")
                .filterBounds(roi)
                .mean()
            )

            # Get soil moisture
            soil_moisture_band = era5.select("volumetric_soil_water_layer_1")
            soil_moisture = soil_moisture_band.reduceRegion(reducer=ee.Reducer.mean(), geometry=roi, scale=10000).get("volumetric_soil_water_layer_1")

            # Get temperature and humidity
            temperature_kelvin = era5.select("temperature_2m")
            dewpoint_kelvin = era5.select("dewpoint_temperature_2m")
            
            temperature_c = temperature_kelvin.subtract(273.15).rename("temperature_C")
            rh = era5.expression(
                "100 * (exp((17.625 * (dew - 273.15)) / (243.04 + (dew - 273.15))) / "
                "exp((17.625 * (temp - 273.15)) / (243.04 + (temp - 273.15))))",
                {"dew": dewpoint_kelvin, "temp": temperature_kelvin},
            ).rename("relative_humidity")
            
            # Reduce all bands to a single dictionary
            data = sentinel.addBands(ndvi).addBands(evi).addBands(temperature_c).addBands(rh)
            
            results = data.reduceRegion(reducer=ee.Reducer.mean(), geometry=roi, scale=1000)

            return {
                "location": location["name"],
                "lat": location["lat"],
                "lon": location["lon"],
                "ndvi": results.get("NDVI").getInfo(),
                "evi": results.get("EVI").getInfo(),
                "soil_moisture": soil_moisture.getInfo(),
                "temperature_C": results.get("temperature_C").getInfo(),
                "relative_humidity": results.get("relative_humidity").getInfo(),
                "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            }
        except Exception as e:
            print(f"Error fetching remote sensing data for {location['name']}: {e}")
            return None
    
    # --- Main Loop ---
    # Fetch data for each location and publish to Pub/Sub
    for location in LOCATIONS:
        data = remote_sensing_data(location)
        if data:
            print(f"Publishing data for {location['name']}")
            json_data = json.dumps(data, default=str)
            data_bytes = json_data.encode("utf-8")
            future = publisher.publish(topic_path, data_bytes)
            future.result()

    print("GEE data ingestion completed.")