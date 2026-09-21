# Databricks notebook source
from pyspark.sql import functions as F
from datetime import datetime, timedelta
import yaml

# COMMAND ----------

default_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

dbutils.widgets.text("env", "dev", "1. Environment")
env = dbutils.widgets.get("env")

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

volume_root = f"/Volumes/{catalog_name}/{bronze_schema}/{volume_name}/batch"
landing_base_path = f"{volume_root}/{batch_config['landing_path']}/"
checkpoint_location = f"{volume_root}/{batch_config['checkpoint_path']}"
schema_location = f"{volume_root}/{batch_config['schema_path']}"

csv_sep = batch_config["csv_delimiter"]
metadata_source_name = batch_config["metadata_source"]

# COMMAND ----------

spark.conf.set("spark.databricks.cloudFiles.schemaInference.sampleSize.numFiles", 10000)

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