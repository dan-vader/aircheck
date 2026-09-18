CREATE SCHEMA IF NOT EXISTS dbr_dev_ua5816bd.aircheck_bronze
  COMMENT 'Raw ingestion and metadata';
CREATE SCHEMA IF NOT EXISTS dbr_dev_ua5816bd.aircheck_silver
  COMMENT 'Cleaned, typed, long-format readings';
CREATE SCHEMA IF NOT EXISTS dbr_dev_ua5816bd.aircheck_gold
  COMMENT 'Aggregates for dashboard';
CREATE SCHEMA IF NOT EXISTS dbr_dev_ua5816bd.aircheck_ops
  COMMENT 'Pipeline run logs';
