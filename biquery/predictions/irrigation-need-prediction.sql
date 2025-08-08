SELECT
  *
FROM
  ML.PREDICT(MODEL `precision_dataset.irrigation_need_model`,
    (SELECT * FROM `precision_dataset.new_data`));       