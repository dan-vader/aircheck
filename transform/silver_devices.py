# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.window import Window
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
bronze_schema = config["schemas"]["bronze"]
silver_schema = config["schemas"]["silver"]

live_raw_table_name = config["streaming"]["target_table"]
live_raw_table = f"{catalog_name}.{bronze_schema}.{live_raw_table_name}"

registry_raw_table_name = config["batch"]["registry_table"]
registry_raw_table = f"{catalog_name}.{bronze_schema}.{registry_raw_table_name}"

devices_silver_table = f"{catalog_name}.{silver_schema}.devices"

# COMMAND ----------

df_registry = spark.table(registry_raw_table)

df_live = spark.table(live_raw_table)
window_spec = Window.partitionBy("sensor_id").orderBy(F.col("timestamp").desc())

df_live_latest = (
    df_live
        .filter(F.col("sensor_id").isNotNull())
        .withColumn("row_num", F.row_number().over(window_spec))
        .filter(F.col("row_num") == 1)
        .select(
            F.col("sensor_id").alias("live_sensor_id"),
            F.col("latitude").alias("live_lat"),
            F.col("longitude").alias("live_lon"),
            F.col("country").alias("live_country")
        )
)

# COMMAND ----------

df_updates = (
    df_registry.alias("reg")
    .join(
        df_live_latest.alias("live"), 
        on=F.col("reg.sensor_id") == F.col("live.live_sensor_id"), 
        how="left"
    )
    .withColumn("lat_resolved", F.coalesce(F.col("live.live_lat"), F.col("reg.lat")))
    .withColumn("lon_resolved", F.coalesce(F.col("live.live_lon"), F.col("reg.lon")))
    .withColumn("country_resolved", F.coalesce(F.col("live.live_country"), F.col("reg.country")))
    .withColumn("country", F.upper(F.col("country_resolved")))
    .withColumn("lat", F.col("lat_resolved").cast("double"))
    .withColumn("lon", F.col("lon_resolved").cast("double"))
    .withColumn("indoor", F.col("reg.indoor").cast("boolean"))
    .select(
        F.col("reg.sensor_id"),
        F.col("reg.sensor_type"),
        F.col("reg.location_id"),
        "country",
        "lat",
        "lon",
        "indoor",
        F.col("reg._source"),
        F.col("reg._ingested_at")
    )
)

df_updates.createOrReplaceTempView("stage_devices_updates_view")

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{silver_schema}")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {devices_silver_table} (
        sensor_id STRING,
        sensor_type STRING,
        location_id STRING,
        country STRING,
        lat DOUBLE,
        lon DOUBLE,
        indoor BOOLEAN,
        _source STRING,
        _ingested_at TIMESTAMP
    )
    USING DELTA
""")

# COMMAND ----------

spark.sql(f"""
    MERGE INTO {devices_silver_table} t
    USING stage_devices_updates_view s
        ON t.sensor_id = s.sensor_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")