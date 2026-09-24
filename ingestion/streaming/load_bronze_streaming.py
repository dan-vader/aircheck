# Databricks notebook source
dbutils.widgets.text("env", "dev")
env = dbutils.widgets.get("env")

dbutils.widgets.text("config_path", "../../config/aircheck.yaml")
config_path = dbutils.widgets.get("config_path")

dbutils.widgets.text("sensor_filter", "")
sensor_filter_override = dbutils.widgets.get("sensor_filter")

# COMMAND ----------

import yaml
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

from common.logging_utils import get_logger, new_batch_id, log_run, log_error

# COMMAND ----------

with open(config_path, "r", encoding="utf-8") as f:
    full_cfg = yaml.safe_load(f)
cfg = full_cfg[env]

catalog = cfg["catalog"]
bronze_schema = cfg["schemas"]["bronze"]
ops_schema = cfg["schemas"]["ops"]

JOB_NAME = "stream.bronze"
log = get_logger(JOB_NAME)
run_batch_id = new_batch_id() 
eventhub_namespace = cfg["eventhub_namespace"]
eventhub_name = cfg["eventhub_name"]
secret_scope = cfg["secret_scope"]
eventhub_conn_str_key = cfg["secrets"]["eventhub_conn_str"]

streaming_cfg = cfg["streaming"]
checkpoint_path = streaming_cfg["checkpoint_path"]
target_table = streaming_cfg["target_table"]
max_offsets_per_trigger = streaming_cfg["max_offsets_per_trigger"]
base_measurement_fields = streaming_cfg["base_measurement_fields"]
phase_b_extra_fields = streaming_cfg["phase_b_extra_fields"]

target_full_name = f"{catalog}.{bronze_schema}.{target_table}"

sensor_filter = sensor_filter_override or streaming_cfg["sensor_type_filter"]

# COMMAND ----------

eh_conn_str = dbutils.secrets.get(scope=secret_scope, key=eventhub_conn_str_key)

kafka_options = {
    "kafka.bootstrap.servers": f"{eventhub_namespace}.servicebus.windows.net:9093",
    "subscribe": eventhub_name,
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.mechanism": "PLAIN",
    "kafka.sasl.jaas.config": (
        "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required "
        f'username="$ConnectionString" password="{eh_conn_str}";'
    ),
    "startingOffsets": "earliest",
    "maxOffsetsPerTrigger": str(max_offsets_per_trigger),
}

# COMMAND ----------

common_fields = [
    StructField("id", StringType()),
    StructField("timestamp", StringType()),
    StructField("sensor_id", StringType()),
    StructField("sensor_type", StringType()),
    StructField("location_id", StringType()),
    StructField("latitude", StringType()),
    StructField("longitude", StringType()),
    StructField("country", StringType()),
]

base_measurement_struct_fields = [StructField(f, StringType()) for f in base_measurement_fields]
phase_b_struct_fields = [StructField(f, StringType()) for f in phase_b_extra_fields]

schema_phase_a = StructType(common_fields + base_measurement_struct_fields)
schema_phase_b = StructType(common_fields + base_measurement_struct_fields + phase_b_struct_fields)

active_schema = schema_phase_b if "BME280" in sensor_filter.upper() else schema_phase_a

# COMMAND ----------

raw = spark.readStream.format("kafka").options(**kafka_options).load()

parsed = (
    raw.select(
        F.col("partition").cast("string").alias("_source_partition"),
        F.col("offset").cast("string").alias("_source_offset"),
        F.from_json(
            F.col("value").cast("string"),
            active_schema,
            {"rescuedDataColumn": "_rescued_data"},
        ).alias("j"),
    )
    .select("_source_partition", "_source_offset", "j.*")
    .withColumn("_source", F.lit("sensor_live_eh"))
    .withColumn("_ingested_at", F.current_timestamp())
)

# COMMAND ----------

log.info("start target=%s schema=%s", target_full_name,
         "phase_b" if active_schema is schema_phase_b else "phase_a")
log_run(spark, catalog=catalog, ops_schema=ops_schema, job_name=JOB_NAME,
        batch_id=run_batch_id, level="INFO",
        message=f"stream starting, schema={'phase_b' if active_schema is schema_phase_b else 'phase_a'}",
        table=target_table)

_last_known_columns = None 


def write_microbatch(microbatch_df, microbatch_id: int) -> None:
    global _last_known_columns
    try:
        count = microbatch_df.count()
        if count == 0:
            return

        current_columns = set(microbatch_df.columns)
        if _last_known_columns is not None and current_columns != _last_known_columns:
            new_cols = current_columns - _last_known_columns
            log.warning("schema change detected in microbatch %d: new columns=%s",
                        microbatch_id, sorted(new_cols))
            log_run(spark, catalog=catalog, ops_schema=ops_schema, job_name=JOB_NAME,
                    batch_id=run_batch_id, level="WARN",
                    message=f"schema change detected, new columns={sorted(new_cols)}",
                    table=target_table)
        _last_known_columns = current_columns

        (
            microbatch_df.write
            .format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .saveAsTable(target_full_name)
        )

        log.info("microbatch %d written rows=%d", microbatch_id, count)
        log_run(spark, catalog=catalog, ops_schema=ops_schema, job_name=JOB_NAME,
                batch_id=run_batch_id, level="INFO",
                message=f"microbatch {microbatch_id} written",
                rows_affected=count, table=target_table)
    except Exception as e:
        log.error("microbatch %d failed: %s", microbatch_id, e)
        log_error(spark, catalog=catalog, ops_schema=ops_schema, job_name=JOB_NAME,
                  batch_id=run_batch_id, message=f"microbatch {microbatch_id} failed", exc=e,
                  table=target_table)
        raise


query = (
    parsed.writeStream
    .option("checkpointLocation", checkpoint_path)
    .outputMode("append")
    .queryName(f"aircheck_{target_table}")
    .trigger(processingTime="30 seconds")
    .foreachBatch(write_microbatch)
    .start()
)

try:
    query.awaitTermination()
except Exception as e:
    log_error(spark, catalog=catalog, ops_schema=ops_schema, job_name=JOB_NAME,
              batch_id=run_batch_id, message="stream terminated with error", exc=e,
              table=target_table)
    raise