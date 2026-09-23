# Databricks notebook source
import yaml

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

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{gold_schema}")

# COMMAND ----------

spark.sql(f"""
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
    WHERE d.indoor = FALSE OR d.indoor IS NULL
    GROUP BY d.country
        , DATE_TRUNC('hour', r.event_ts_utc)
        , r.value_type
""")

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE TABLE {catalog_name}.{gold_schema}.air_quality_daily_by_country AS
    SELECT d.country
        , DATE(r.event_ts_utc) AS day
        , r.value_type
        , AVG(r.value) AS avg_value
        , COUNT(*) AS n_readings
    FROM {catalog_name}.{silver_schema}.readings r
    JOIN {catalog_name}.{silver_schema}.devices d ON r.sensor_id = d.sensor_id
    WHERE d.indoor = FALSE OR d.indoor IS NULL
    GROUP BY d.country
        , DATE(r.event_ts_utc)
        , r.value_type
""")

# COMMAND ----------

spark.sql(f"""
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

spark.sql(f"""
    CREATE OR REPLACE TABLE {catalog_name}.{gold_schema}.anomalies AS
    SELECT r.sensor_id
        , r.event_ts_utc
        , r.value_type
        , r.value
        , r.location_id
        , COALESCE(r.country, d.country) AS country
        , d.indoor
        , CASE
            WHEN r.value_type = 'P1' AND r.value > 50 THEN 'PM10_EXCEEDS_WHO_DAILY'
            WHEN r.value_type = 'P2' AND r.value > 25 THEN 'PM2_5_EXCEEDS_WHO_DAILY'
            ELSE NULL
        END AS anomaly_flag
    FROM {catalog_name}.{silver_schema}.readings r
    JOIN {catalog_name}.{silver_schema}.devices d ON r.sensor_id = d.sensor_id
    WHERE r.value_type IN ('P1', 'P2')
        AND r.value IS NOT NULL
""")