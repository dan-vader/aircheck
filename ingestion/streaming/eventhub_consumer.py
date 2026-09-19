# Databricks notebook source
dbutils.widgets.text("env", "dev")
env = dbutils.widgets.get("env")

dbutils.widgets.text("sensor_filter", "")
sensor_filter_override = dbutils.widgets.get("sensor_filter")

import yaml
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

dbutils.widgets.text("config_path", "../../config/aircheck.yaml")
config_path = dbutils.widgets.get("config_path")

with open(config_path, "r", encoding="utf-8") as f:
    full_cfg = yaml.safe_load(f)
cfg = full_cfg[env]

catalog = cfg["catalog"]
bronze_schema = cfg["schemas"]["bronze"]
eventhub_namespace = cfg["eventhub_namespace"]
eventhub_name = cfg["eventhub_name"]
secret_scope = cfg["secret_scope"]
eventhub_conn_str_key = cfg["secrets"]["eventhub_conn_str"]

streaming_cfg = cfg["streaming"]
checkpoint_path = streaming_cfg["checkpoint_path"]
target_table = streaming_cfg["target_table"]
max_offsets_per_trigger = streaming_cfg.get("max_offsets_per_trigger", 5000)

target_full_name = f"{catalog}.{bronze_schema}.{target_table}"

sensor_filter = sensor_filter_override or streaming_cfg["sensor_type_filter"]

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

schema_phase_a = StructType([
    StructField("id", StringType()),
    StructField("timestamp", StringType()),
    StructField("sensor_id", StringType()),
    StructField("sensor_type", StringType()),
    StructField("location_id", StringType()),
    StructField("latitude", StringType()),
    StructField("longitude", StringType()),
    StructField("country", StringType()),
    StructField("P1", StringType()),
    StructField("P2", StringType()),
])

schema_phase_b = StructType(
    schema_phase_a.fields
    + [
        StructField("temperature", StringType()),
        StructField("humidity", StringType()),
        StructField("pressure", StringType()),
    ]
)

active_schema = schema_phase_b if "BME280" in sensor_filter.upper() else schema_phase_a

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

query = (
    parsed.writeStream
    .option("checkpointLocation", checkpoint_path)
    .option("mergeSchema", "true")
    .outputMode("append")
    .queryName(f"aircheck_{target_table}")
    .trigger(processingTime="30 seconds")
    .toTable(target_full_name)
)

query.awaitTermination()