# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %sql
# MAGIC SELECT COUNT(*) AS total_rows,
# MAGIC        MIN(_ingested_at) AS first_ingested,
# MAGIC        MAX(_ingested_at) AS last_ingested
# MAGIC FROM dbr_dev_ua5816bd.aircheck_bronze.sensor_live_raw;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM dbr_dev_ua5816bd.aircheck_bronze.sensor_live_raw
# MAGIC ORDER BY _ingested_at DESC
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS rescued_count
# MAGIC FROM dbr_dev_ua5816bd.aircheck_bronze.sensor_live_raw
# MAGIC WHERE _rescued_data IS NOT NULL;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT _source_partition,
# MAGIC        COUNT(*) AS cnt,
# MAGIC        MAX(_ingested_at) AS last_seen
# MAGIC FROM dbr_dev_ua5816bd.aircheck_bronze.sensor_live_raw
# MAGIC GROUP BY _source_partition
# MAGIC ORDER BY _source_partition;

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS dbr_dev_ua5816bd.roksolana_shendiu770_bronze.petroleum_consumption_raw_ldp_bronze")
spark.sql("DROP TABLE IF EXISTS dbr_dev_ua5816bd.roksolana_shendiu770_bronze.petroleum_prices_raw_ldp_bronze")