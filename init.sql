CREATE TABLE transactions (
    date String,
    project_id String,
    number_of_transactions Int32,
    total_volume_usd Float64
) ENGINE = ReplacingMergeTree()
ORDER BY (date, project_id);

