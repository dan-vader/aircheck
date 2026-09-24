CREATE TABLE IF NOT EXISTS dbr_dev_ua5816bd.aircheck_ops.pipeline_log (
    job_name       STRING,
    batch_id       STRING,
    level          STRING,        -- INFO / WARN / ERROR
    message        STRING,
    rows_affected  BIGINT,
    target_table   STRING,
    logged_at      TIMESTAMP
)
COMMENT 'Append-only run log written by common/logging_utils.py from every job';
