import ee
import json
import datetime
import os
import pandas as pd
from google.cloud import pubsub_v1

PROJECT_ID = "careful-trainer-p1"
# TOPIC_ID = "earth-engine-data-pub"

# publisher = pubsub_v1.PublisherClient()
# topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)


# 1. Authenticate and Initialize Earth Engine
# You only need to run this authentication step once.
# After running, follow the link to log in with your Google account.
#ee.Authenticate()

ee.Initialize(project=PROJECT_ID)  # Initialize Earth Engine with your project ID

# Define cloud masking function
def mask_s2_clouds(image):
    """Masks clouds in a Sentinel-2 image using the QA band.

  Args:
      image (ee.Image): A Sentinel-2 image.

  Returns:
      ee.Image: A cloud-masked Sentinel-2 image.
  """
    qa = image.select('QA60')
    
    # Bits 10 and 11 are clouds and cirrus, respectively.
    cloud_bit_mask = 1 << 10
    cirrus_bit_mask = 1 << 11
    
    # Both flags should be set to zero, indicating clear conditions.
    mask = qa.bitwiseAnd(cloud_bit_mask).eq(0).And(
        qa.bitwiseAnd(cirrus_bit_mask).eq(0)
    )
    
    return image.updateMask(mask).divide(10000)  # Optional: scale reflectance to 0–1


# Define ROI
roi = ee.Geometry.Point([85.84, 20.27])  # Odisha example

# Load and prepare Sentinel-2 data
sentinel = ee.ImageCollection('COPERNICUS/S2_HARMONIZED') \
    .filterDate('2025-05-01', '2025-06-01') \
    .filterBounds(roi) \
    .filter(ee.Filter.lt('CLOUDY_PIXEL_PERCENTAGE', 20)) \
    .map(mask_s2_clouds) \
    .select(['B2', 'B4', 'B8'])  # Blue, Red, NIR

# Aggregate the image
image = sentinel.median()

# Calculate NDVI
ndvi = image.normalizedDifference(['B8', 'B4']).rename('NDVI')

# Calculate EVI
evi = image.expression(
    '2.5 * ((NIR - RED) / (NIR + 6 * RED - 7.5 * BLUE + 1))',
    {
        'NIR': image.select('B8'),
        'RED': image.select('B4'),
        'BLUE': image.select('B2')
    }
).rename('EVI')

# Extract mean values at ROI
ndvi_mean = ndvi.reduceRegion(
    reducer=ee.Reducer.mean(), geometry=roi, scale=10
)
evi_mean = evi.reduceRegion(
    reducer=ee.Reducer.mean(), geometry=roi, scale=10
)

print("NDVI:", ndvi_mean.getInfo())
print("EVI:", evi_mean.getInfo())



