# Create and train the Crop Yield Prediction Model (Regression)
CREATE OR REPLACE MODEL
  `precision_dataset.yield_prediction_model`
OPTIONS(
  model_type='LINEAR_REG',
  input_label_cols=['yield_tph']
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
  yield_tph
FROM
  `precision_dataset.training_data_labeled`
WHERE
  yield_tph IS NOT NULL;