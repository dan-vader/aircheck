# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
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

archive_raw_table_name = config["batch"]["target_table"]
archive_raw_table = f"{catalog_name}.{bronze_schema}.{archive_raw_table_name}"

live_raw_table_name = config["streaming"]["target_table"]
live_raw_table = f"{catalog_name}.{bronze_schema}.{live_raw_table_name}"

readings_silver_table = f"{catalog_name}.{silver_schema}.readings"

# COMMAND ----------

METRICS = config["batch"]["target_metrics"]
ID_COLS = [
    "sensor_id", "timestamp", "location_id", 
    "latitude", "longitude", "country", "_source", "_ingested_at"
]

# COMMAND ----------

def unpivot_metrics(df: DataFrame) -> DataFrame:
    present_metrics = [m for m in METRICS if m in df.columns]
    present_ids = [i for i in ID_COLS if i in df.columns]

    return (
        df.unpivot(
            ids=present_ids,
            values=present_metrics,
            variableColumnName="value_type",
            valueColumnName="raw_value"
        )
        .filter(F.col("raw_value").isNotNull())
        .withColumn("value", F.expr("try_cast(raw_value as double)"))
        .drop("raw_value")
        .filter(F.col("value").isNotNull())
    )

# COMMAND ----------

df_archive_raw = spark.table(archive_raw_table)

df_archive_clean = (
    df_archive_raw
        .withColumnRenamed("location", "location_id")
        .withColumnRenamed("lat", "latitude")
        .withColumnRenamed("lon", "longitude")
        .withColumn("country", F.lit(None).cast("string"))
)

df_live_raw = spark.table(live_raw_table)

# COMMAND ----------

df_archive_long = unpivot_metrics(df_archive_clean)
df_live_long = unpivot_metrics(df_live_raw)

# COMMAND ----------

readings_silver = (
    df_live_long.unionByName(df_archive_long, allowMissingColumns=True)
        .withColumn("sensor_id", F.col("sensor_id").cast("string"))
        .withColumn("location_id", F.col("location_id").cast("string"))
        .withColumn("event_ts_utc", F.to_timestamp(F.regexp_replace("timestamp", "['\"]", "")))
        .withColumn("latitude", F.col("latitude").cast("double"))
        .withColumn("longitude", F.col("longitude").cast("double"))
        .withColumn("country", F.upper(F.col("country")))
        .dropDuplicates(["sensor_id", "event_ts_utc", "value_type"])
        .select(
            "sensor_id",
            "event_ts_utc",
            "value_type",
            "value",
            "location_id",
            "latitude",
            "longitude",
            "country",
            "_source",
            "_ingested_at"
        )
)

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{silver_schema}")

(
    readings_silver.write
        .format("delta")
        .mode("overwrite")
        .option("mergeSchema", "true")
        .saveAsTable(readings_silver_table)
)