SELECT
    column_name AS name,
    data_type AS type,
    IF(is_nullable = 'YES', 'NULLABLE', 'REQUIRED') AS mode
FROM
    `careful-trainer-p1.precision_dataset`.INFORMATION_SCHEMA.COLUMNS
WHERE
    table_name = 'training_data_labeled'
ORDER BY
    ordinal_position;