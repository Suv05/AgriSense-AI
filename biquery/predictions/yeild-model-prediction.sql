SELECT
  *
FROM
  ML.PREDICT(MODEL `precision_dataset.yield_prediction_model`,
    (SELECT * FROM `precision_dataset.new_data`));