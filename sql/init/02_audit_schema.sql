CREATE SCHEMA IF NOT EXISTS audit;

CREATE TABLE IF NOT EXISTS audit.partition_loads (
    partition_key VARCHAR(50) PRIMARY KEY,
    loaded_at_utc TIMESTAMP WITH TIME ZONE,
    row_count INT NOT NULL,
    pipeline_run_id VARCHAR(100) NOT NULL
);
