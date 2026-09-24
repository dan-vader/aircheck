# Databricks notebook source
from pyspark.sql import functions as F
import requests
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
secret_scope = config["secret_scope"]
contact_email_key = config["secrets"]["contact_email"]
contact_email = dbutils.secrets.get(secret_scope, contact_email_key)
headers = {"User-Agent": f"databricks-lab-team (contact: {contact_email})"}

batch_config = config["batch"]
catalog_name = config["catalog"]
bronze_schema = config["schemas"]["bronze"]
ops_schema = config["schemas"]["ops"]
registry_table_name = batch_config["registry_table"]
registry_table = f"{catalog_name}.{bronze_schema}.{registry_table_name}"

registry_config = config["batch"]["registry"]
COUNTRIES = registry_config["countries"]
ALLOWED_SENSORS = registry_config["allowed_sensors"]
source_name = registry_config["source_name"]
source_file_name = registry_config["source_file_name"]
timeout_sec = registry_config["request_timeout"]
base_url = registry_config["api_base_url"]

JOB_NAME = "batch.device_registry"
log = get_logger(JOB_NAME)
batch_id = new_batch_id()

log.info("start env=%s countries=%s", env, COUNTRIES)
log_run(spark, catalog=catalog_name, ops_schema=ops_schema, job_name=JOB_NAME,
        batch_id=batch_id, level="INFO", message=f"start env={env} countries={COUNTRIES}")

# COMMAND ----------

try:
    url = f"{base_url}{','.join(COUNTRIES)}"
    response = requests.get(url, headers=headers, timeout=timeout_sec)
    response.raise_for_status()
    records = response.json()
except Exception as e:
    log.error("failed to fetch live API: %s", e)
    log_error(spark, catalog=catalog_name, ops_schema=ops_schema, job_name=JOB_NAME,
              batch_id=batch_id, message="failed to fetch live API", exc=e,
              table=registry_table_name)
    raise

# COMMAND ----------

registry = {}

for rec in records:
    loc = rec.get("location", {})
    sensor_info = rec.get("sensor", {})

    sensor_id = str(sensor_info.get("id"))
    sensor_type = sensor_info.get("sensor_type", {}).get("name")

    if sensor_id and sensor_id not in registry:
        if sensor_type in ALLOWED_SENSORS:
            registry[sensor_id] = {
                "sensor_id": sensor_id,
                "sensor_type": sensor_type,
                "location_id": str(loc.get("id")),
                "country": loc.get("country"),
                "latitude": str(loc.get("latitude")),
                "longitude": str(loc.get("longitude")),
                "indoor": str(loc.get("indoor", "0")),
            }

print(f"Records in API: {len(records)}")
print(f"Records saved: {len(registry)}")

# COMMAND ----------

api_records = list(registry.values())
df_api = spark.createDataFrame(api_records)

df_staging = (
    df_api
        .withColumnRenamed("latitude", "lat")
        .withColumnRenamed("longitude", "lon")
        .withColumn("_source", F.lit(source_name))
        .withColumn("_source_file_name", F.lit(source_file_name))
        .withColumn("_ingested_at", F.current_timestamp())
)

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {registry_table} (
        sensor_id STRING,
        sensor_type STRING,
        location_id STRING,
        lat STRING,
        lon STRING,
        country STRING,
        indoor STRING,
        _source STRING,
        _source_file_name STRING,
        _ingested_at TIMESTAMP
    )
""")

# COMMAND ----------

df_staging.createOrReplaceTempView("api_staging_devices")

try:
    spark.sql(f"""
        MERGE INTO {registry_table} t
        USING api_staging_devices s
            ON t.sensor_id = s.sensor_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
except Exception as e:
    log.error("device registry merge failed: %s", e)
    log_error(spark, catalog=catalog_name, ops_schema=ops_schema, job_name=JOB_NAME,
              batch_id=batch_id, message="device registry merge failed", exc=e,
              table=registry_table_name)
    raise

n = len(api_records)
log.info("device registry merge done rows=%d", n)
log_run(spark, catalog=catalog_name, ops_schema=ops_schema, job_name=JOB_NAME,
        batch_id=batch_id, level="INFO", message="device registry merge done",
        rows_affected=n, table=registry_table_name)