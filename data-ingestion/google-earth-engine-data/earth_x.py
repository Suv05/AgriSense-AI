import ee
import json
import datetime
import logging
from google.cloud import pubsub_v1

PROJECT_ID = "careful-trainer-p1"
TOPIC_ID = "earth-engine-data-pub"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)


# 1. Authenticate and Initialize Earth Engine
# You only need to run this authentication step once.
# After running, follow the link to log in with your Google account.
# ee.Authenticate()

ee.Initialize(project=PROJECT_ID)  # Initialize Earth Engine with your project ID


# Define cloud masking function
def mask_s2_clouds(image):
    """Masks clouds in a Sentinel-2 image using the QA band.

    Args:
        image (ee.Image): A Sentinel-2 image.

    Returns:
        ee.Image: A cloud-masked Sentinel-2 image.
    """
    qa = image.select("QA60")

    # Bits 10 and 11 are clouds and cirrus, respectively.
    cloud_bit_mask = 1 << 10
    cirrus_bit_mask = 1 << 11

    # Both flags should be set to zero, indicating clear conditions.
    mask = qa.bitwiseAnd(cloud_bit_mask).eq(0).And(qa.bitwiseAnd(cirrus_bit_mask).eq(0))

    return image.updateMask(mask).divide(10000)  # Optional: scale reflectance to 0–1


def remote_sensing_data(location):
    """Fetches remote sensing data for a given region of interest (ROI)."""

    # Define ROI
    roi = ee.Geometry.Point([location["lon"], location["lat"]])
    # Load and prepare Sentinel-2 data
    try:
        sentinel = (
            ee.ImageCollection("COPERNICUS/S2_HARMONIZED")
            .filterDate("2025-07-05", "2025-08-05")
            .filterBounds(roi)
            .filter(ee.Filter.lt("CLOUDY_PIXEL_PERCENTAGE", 20))
            .map(mask_s2_clouds)
            .select(["B2", "B4", "B8"])
        )  # Blue, Red, NIR

        # Aggregate the image
        image = sentinel.median()

        # Calculate NDVI
        ndvi = image.normalizedDifference(["B8", "B4"]).rename("NDVI")

        # Calculate EVI
        evi = image.expression(
            "2.5 * ((NIR - RED) / (NIR + 6 * RED - 7.5 * BLUE + 1))",
            {
                "NIR": image.select("B8"),
                "RED": image.select("B4"),
                "BLUE": image.select("B2"),
            },
        ).rename("EVI")

        # Extract mean values at ROI
        ndvi_mean = ndvi.reduceRegion(reducer=ee.Reducer.mean(), geometry=roi, scale=10)
        evi_mean = evi.reduceRegion(reducer=ee.Reducer.mean(), geometry=roi, scale=10)

        print("NDVI:", ndvi_mean.getInfo())
        print("EVI:", evi_mean.getInfo())

        # pull SMAP soil moisture data
        smap = (
            ee.ImageCollection("ECMWF/ERA5_LAND/HOURLY")
            .filterDate("2025-07-01", "2025-07-31")
            .filterBounds(roi)
        )

        smap_image = smap.mean()

        soil_moisture = smap_image.select("volumetric_soil_water_layer_1").reduceRegion(
            reducer=ee.Reducer.mean(), geometry=roi, scale=10000
        )

        print("Soil Moisture (ERA5):", soil_moisture.getInfo())

        # humidity and temperature
        era5 = (
            ee.ImageCollection("ECMWF/ERA5_LAND/HOURLY")
            .filterDate("2025-07-01", "2025-07-31")
            .filterBounds(roi)
        )

        # Take mean over the month
        era5_image = era5.mean()

        # Select temperature and dewpoint in Kelvin
        temperature = era5_image.select("temperature_2m")
        dewpoint = era5_image.select("dewpoint_temperature_2m")

        # Calculate Relative Humidity using formula:
        # RH = 100 * (exp((17.625 * dew)/(243.04 + dew)) / exp((17.625 * temp)/(243.04 + temp)))
        rh = era5_image.expression(
            """
            100 * (
                exp((17.625 * (dew - 273.15)) / (243.04 + (dew - 273.15))) /
                exp((17.625 * (temp - 273.15)) / (243.04 + (temp - 273.15)))
            )
            """,
            {"dew": dewpoint, "temp": temperature},
        ).rename("relative_humidity")

        # Reduce at the point
        temperature_c = temperature.subtract(273.15).rename(
            "temperature_C"
        )  # Kelvin to °C

        results = temperature_c.addBands(rh).reduceRegion(
            reducer=ee.Reducer.mean(), geometry=roi, scale=1000
        )

        print("Temperature and Humidity (ERA5):", results.getInfo())

        return {
            "location": location["name"],
            "lat": location["lat"],
            "lon": location["lon"],
            "ndvi": ndvi_mean.getInfo().get("NDVI"),
            "evi": evi_mean.getInfo().get("EVI"),
            "soil_moisture": soil_moisture.getInfo().get(
                "volumetric_soil_water_layer_1"
            ),
            "temperature_C": results.getInfo().get("temperature_C"),
            "relative_humidity": results.getInfo().get("relative_humidity"),
            "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        }
    except Exception as e:
        print(f"Error fetching remote sensing data: {e}")
        return None


# Load config.json for locations
with open("./data-ingestion/google-earth-engine-data/config.json", "r") as f:
    LOCATIONS = json.load(f)


for location in LOCATIONS:

    data = remote_sensing_data(location)
    if data:
        logging.info(f"Publishing data for {location['name']}")
        future = publisher.publish(
            topic_path, json.dumps(data, default=str).encode("utf-8")
        )
        future.result()
