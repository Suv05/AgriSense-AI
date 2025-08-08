# Combine predictions from all three models into a single table
CREATE OR REPLACE TABLE `precision_dataset.final_table` AS
WITH
  YieldPredictions AS (
    SELECT
      location,
      lat,
      lon,
      predicted_yield_tph
    FROM
      ML.PREDICT(MODEL `precision_dataset.yield_prediction_model`,
        (SELECT * FROM `precision_dataset.new_data`))
  ),
  IrrigationPredictions AS (
    SELECT
      location,
      lat,
      lon,
      predicted_irrigation_need,
      predicted_irrigation_need_probs # Keep probabilities for more detail if needed
    FROM
      ML.PREDICT(MODEL `precision_dataset.irrigation_need_model`,
        (SELECT * FROM `precision_dataset.new_data`))
  ),
  DiseaseRiskPredictions AS (
    SELECT
      location,
      lat,
      lon,
      predicted_disease_risk
    FROM
      ML.PREDICT(MODEL `precision_dataset.disease_risk_model`,
        (SELECT * FROM `precision_dataset.new_data`))
  )
SELECT
  nd.location,
  nd.lat,
  nd.lon,
  yp.predicted_yield_tph,
  ip.predicted_irrigation_need,
  ip.predicted_irrigation_need_probs,
  drp.predicted_disease_risk,
  -- Include any other relevant input features from new_data if you want them in the final table
  nd.ndvi,
  nd.evi,
  nd.soil_moisture,
  nd.weather_temp,
  nd.weather_humidity,
  nd.pressure,
  nd.wind_speed,
  nd.wind_deg
FROM
  `precision_dataset.new_data` AS nd # Start with your original new data to ensure all locations are covered
LEFT JOIN
  YieldPredictions AS yp
ON
  nd.location = yp.location AND nd.lat = yp.lat AND nd.lon = yp.lon
LEFT JOIN
  IrrigationPredictions AS ip
ON
  nd.location = ip.location AND nd.lat = ip.lat AND nd.lon = ip.lon
LEFT JOIN
  DiseaseRiskPredictions AS drp
ON
  nd.location = drp.location AND nd.lat = drp.lat AND nd.lon = drp.lon;