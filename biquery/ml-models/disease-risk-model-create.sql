# Create and train the Disease Risk Model (Regression)
CREATE OR REPLACE MODEL
  `precision_dataset.disease_risk_model`
OPTIONS(
  model_type='LINEAR_REG',
  input_label_cols=['disease_risk']
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
  disease_risk
FROM
  `precision_dataset.training_data_labeled`
WHERE
  disease_risk IS NOT NULL;