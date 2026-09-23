# Databricks notebook source
import yaml

from common.logging_utils import get_logger, new_batch_id, log_run, log_error

# COMMAND ----------

dbutils.widgets.text("env", "dev", "1. Environment")
env = dbutils.widgets.get("env")

# COMMAND ----------

CONFIG_PATH = "../config/aircheck.yaml"
with open(CONFIG_PATH, "r") as f:
    full_config = yaml.safe_load(f)

config = full_config[env]
catalog_name = config["catalog"]
silver_schema = config["schemas"]["silver"]
gold_schema = config["schemas"]["gold"]
ops_schema = config["schemas"]["ops"]

JOB_NAME = "gold.aggregates"
log = get_logger(JOB_NAME)
batch_id = new_batch_id()
log.info("start")
log_run(spark, catalog=catalog_name, ops_schema=ops_schema, job_name=JOB_NAME,
        batch_id=batch_id, level="INFO", message="start")


def run_table(name: str, sql: str) -> None:
    """Run one CREATE OR REPLACE TABLE statement, log rows and errors."""
    full_name = f"{catalog_name}.{gold_schema}.{name}"
    try:
        spark.sql(sql)
        n = spark.table(full_name).count()
    except Exception as e:
        log.error("%s failed: %s", name, e)
        log_error(spark, catalog=catalog_name, ops_schema=ops_schema, job_name=JOB_NAME,
                  batch_id=batch_id, message=f"{name} build failed", exc=e, table=full_name)
        raise
    log.info("%s done rows=%d", name, n)
    log_run(spark, catalog=catalog_name, ops_schema=ops_schema, job_name=JOB_NAME,
            batch_id=batch_id, level="INFO", message=f"{name} done",
            rows_affected=n, table=full_name)

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{gold_schema}")

# COMMAND ----------

run_table("air_quality_hourly", f"""
    CREATE OR REPLACE TABLE {catalog_name}.{gold_schema}.air_quality_hourly AS
    SELECT d.country
        , DATE_TRUNC('hour', r.event_ts_utc) AS hour
        , r.value_type
        , AVG(r.value) AS avg_value
        , MIN(r.value) AS min_value
        , MAX(r.value) AS max_value
        , COUNT(*) AS n_readings
    FROM {catalog_name}.{silver_schema}.readings r
    JOIN {catalog_name}.{silver_schema}.devices d ON r.sensor_id = d.sensor_id
    WHERE (d.indoor = FALSE OR d.indoor IS NULL)
    GROUP BY d.country
        , DATE_TRUNC('hour', r.event_ts_utc)
        , r.value_type
""")

# COMMAND ----------

run_table("air_quality_daily_by_country", f"""
    CREATE OR REPLACE TABLE {catalog_name}.{gold_schema}.air_quality_daily_by_country AS
    SELECT d.country
        , DATE(r.event_ts_utc) AS day
        , r.value_type
        , AVG(r.value) AS avg_value
        , COUNT(*) AS n_readings
    FROM {catalog_name}.{silver_schema}.readings r
    JOIN {catalog_name}.{silver_schema}.devices d ON r.sensor_id = d.sensor_id
    WHERE (d.indoor = FALSE OR d.indoor IS NULL)
    GROUP BY d.country
        , DATE(r.event_ts_utc)
        , r.value_type
""")

# COMMAND ----------

run_table("sensor_network_health", f"""
    CREATE OR REPLACE TABLE {catalog_name}.{gold_schema}.sensor_network_health AS
    SELECT d.country
        , COUNT(DISTINCT r.sensor_id)  AS active_sensors
        , MAX(r.event_ts_utc) AS last_seen
        , COUNT(DISTINCT r.value_type) AS distinct_metrics_seen
    FROM {catalog_name}.{silver_schema}.readings r
    JOIN {catalog_name}.{silver_schema}.devices d ON r.sensor_id = d.sensor_id
    GROUP BY d.country
""")

# COMMAND ----------

run_table("anomalies", f"""
    CREATE OR REPLACE TABLE {catalog_name}.{gold_schema}.anomalies AS
    SELECT r.sensor_id
        , r.event_ts_utc
        , r.value_type
        , r.value
        , r.location_id
        /* Prioritize dimension country (d.country), falling back to 
           event country (r.country) for unregistered yet sensors */
        , COALESCE(d.country, r.country) AS country
        , CASE
            WHEN r.value_type = 'P1' AND r.value > 50 THEN 'PM10_EXCEEDS_WHO_DAILY'
            WHEN r.value_type = 'P2' AND r.value > 25 THEN 'PM2_5_EXCEEDS_WHO_DAILY'
            ELSE NULL
        END AS anomaly_flag
    FROM {catalog_name}.{silver_schema}.readings r
    LEFT JOIN {catalog_name}.{silver_schema}.devices d ON r.sensor_id = d.sensor_id
    WHERE r.value_type IN ('P1', 'P2')
        AND r.value IS NOT NULL
        AND (d.indoor = FALSE OR d.indoor IS NULL)
""")