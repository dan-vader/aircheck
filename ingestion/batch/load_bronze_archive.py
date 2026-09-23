# Databricks notebook source
from pyspark.sql import functions as F
import yaml

from common.logging_utils import get_logger, new_batch_id, log_run, log_error

# COMMAND ----------

dbutils.widgets.text("env", "dev", "1. Environment")
env = dbutils.widgets.get("env")

# COMMAND ----------

CONFIG_PATH = "../../config/aircheck.yaml"
with open(CONFIG_PATH, "r") as f:
    full_config = yaml.safe_load(f)

config = full_config[env]
catalog_name = config["catalog"]
bronze_schema = config["schemas"]["bronze"]
ops_schema = config["schemas"]["ops"]
volume_name = config["volume"]

batch_config = config["batch"]

target_table_name = batch_config["target_table"]
target_table = f"{catalog_name}.{bronze_schema}.{target_table_name}"

JOB_NAME = "batch.bronze_archive"
log = get_logger(JOB_NAME)
batch_id = new_batch_id()

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

log.info("start target=%s", target_table)
log_run(spark, catalog=catalog_name, ops_schema=ops_schema, job_name=JOB_NAME,
        batch_id=batch_id, level="INFO", message="start", table=target_table_name)

try:
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

    rows_processed = sum(p.get("numInputRows", 0) for p in write_bronze_archive.recentProgress)
except Exception as e:
    log.error("Auto Loader run failed: %s", e)
    log_error(spark, catalog=catalog_name, ops_schema=ops_schema, job_name=JOB_NAME,
              batch_id=batch_id, message="Auto Loader run failed", exc=e,
              table=target_table_name)
    raise

log.info("done rows=%d", rows_processed)
log_run(spark, catalog=catalog_name, ops_schema=ops_schema, job_name=JOB_NAME,
        batch_id=batch_id, level="INFO", message="done",
        rows_affected=rows_processed, table=target_table_name)