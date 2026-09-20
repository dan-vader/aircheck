# Databricks notebook source
from pyspark.sql import functions as F
from datetime import datetime, timedelta
import yaml

# COMMAND ----------

default_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

dbutils.widgets.text("env", "dev", "1. Environment")
dbutils.widgets.text("batch_date", default_date, "2. Batch Date (YYYY-MM-DD)")

env = dbutils.widgets.get("env")
batch_date = dbutils.widgets.get("batch_date")

# COMMAND ----------

CONFIG_PATH = "../../config/aircheck.yaml"
with open(CONFIG_PATH, "r") as f:
    full_config = yaml.safe_load(f)

config = full_config[env]
catalog_name = config["catalog"]
bronze_schema = config["schemas"]["bronze"]
volume_name = config["volume"]

batch_config = config["batch"]

target_table_name = batch_config["target_table"]
target_table = f"{catalog_name}.{bronze_schema}.{target_table_name}"

registry_table_name = batch_config["registry_table"]
registry_table = f"{catalog_name}.{bronze_schema}.{registry_table_name}"

volume_root = f"/Volumes/{catalog_name}/{bronze_schema}/{volume_name}/batch"
landing_base_path = f"{volume_root}/{batch_config['landing_path']}/"
checkpoint_location = f"{volume_root}/{batch_config['checkpoint_path']}"
schema_location = f"{volume_root}/{batch_config['schema_path']}"

csv_sep = batch_config["csv_delimiter"]
metadata_source_name = batch_config["metadata_source"]

# COMMAND ----------

df_raw = (
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", schema_location)
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("sep", csv_sep)
        .option("header", "true")
        .load(landing_base_path)
)

# COMMAND ----------

df_enriched = (
    df_raw
        .withColumn("_source", F.lit(metadata_source_name))
        .withColumn("_source_file_name", F.col("_metadata.file_path"))
        .withColumn("_ingested_at", F.current_timestamp())
)

# COMMAND ----------

write_bronze_archive = (
    df_enriched.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_location)
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable(target_table)
)

write_bronze_archive.awaitTermination()

# COMMAND ----------

daily_landing_path = f"{landing_base_path}date={batch_date}/"

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {registry_table} (
        sensor_id STRING,
        sensor_type STRING,
        location_id STRING,
        lat STRING,
        lon STRING,
        indoor STRING,
        _source STRING,
        _source_file_name STRING,
        _ingested_at TIMESTAMP
    )
""")

# COMMAND ----------

raw_csv_df = (
    spark.read
        .format("csv")
        .option("header", "true")
        .option("sep", csv_sep)
        .load(daily_landing_path)
)

# COMMAND ----------

df_devices = (
    raw_csv_df
        .filter(F.col("sensor_id").isNotNull())
        .select(
            F.col("sensor_id"),
            F.col("sensor_type"),
            F.col("location").alias("location_id"),
            F.col("lat"),
            F.col("lon"),
            F.when(F.lower(F.col("_metadata.file_path")).contains("indoor"), "1")
                .otherwise("0")
                .alias("indoor")
        )
        .dropDuplicates(["sensor_id"])
        .withColumn("_source", F.lit(metadata_source_name))
        .withColumn("_source_file_name", F.col("_metadata.file_path"))
        .withColumn("_ingested_at", F.current_timestamp())
)

# COMMAND ----------

df_devices.createOrReplaceTempView("staging_devices")

spark.sql(f"""
    MERGE INTO {registry_table} t
    USING staging_devices s
        ON t.sensor_id = s.sensor_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")