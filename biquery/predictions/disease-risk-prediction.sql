SELECT
  *
FROM
  ML.PREDICT(MODEL `precision_dataset.disease_risk_model`,
    (SELECT * FROM `precision_dataset.new_data`));       