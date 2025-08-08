CREATE OR REPLACE TABLE precision_dataset.training_data_labeled AS
SELECT
  *,
  
  -- Synthetic Yield: influenced by NDVI, soil moisture, and temp
  ROUND(5 + (ndvi * 10) + (soil_moisture * 15) - (ABS(weather_temp - 28) * 0.5), 2) AS yield_tph,

  -- Synthetic Irrigation Need: 1 if soil moisture is low and humidity is low
  CASE
    WHEN soil_moisture < 0.2 AND weather_humidity < 60 THEN 1
    ELSE 0
  END AS irrigation_need,

  -- Synthetic Disease Risk: higher if humidity and temperature are high
  ROUND(LEAST(1.0, (weather_humidity / 100.0) * (GREATEST(weather_temp - 25, 0) / 10.0)), 2) AS disease_risk

FROM
    `careful-trainer-p1.precision_dataset.training_data`
WHERE
  ndvi IS NOT NULL AND soil_moisture IS NOT NULL AND weather_temp IS NOT NULL
