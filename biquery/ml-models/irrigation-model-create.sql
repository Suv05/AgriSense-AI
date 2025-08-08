# Create and train the Irrigation Needs Model (Classification)
CREATE OR REPLACE MODEL
  `precision_dataset.irrigation_need_model`
OPTIONS(
  model_type='LOGISTIC_REG',
  input_label_cols=['irrigation_need']
) AS
SELECT
  ndvi,
  evi,
  soil_moisture,
  satellite_temp,
  satellite_rh,
  weather_temp,
  weather_humidity,
  pressure,
  wind_speed,
  wind_deg,
  wind_gust,
  irrigation_need
FROM
  `precision_dataset.training_data_labeled`
WHERE
  irrigation_need IS NOT NULL;