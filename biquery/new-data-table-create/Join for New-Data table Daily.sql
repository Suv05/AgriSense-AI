CREATE OR REPLACE TABLE `careful-trainer-p1.precision_dataset.new_data` AS
SELECT
  w.location,
  w.lat,
  w.lon,
  s.ndvi,
  s.evi,
  s.soil_moisture,
  s.temperature_C AS satellite_temp,
  s.relative_humidity AS satellite_rh,
  w.temp AS weather_temp,
  w.humidity AS weather_humidity,
  w.pressure,
  w.wind_speed,
  w.wind_deg,
  w.wind_gust
FROM
  `careful-trainer-p1.precision_dataset.weather_data` AS w
LEFT JOIN
  `careful-trainer-p1.precision_dataset.satelite_data` AS s
ON
  w.location = s.location
  AND w.lat = s.lat
  AND w.lon = s.lon;