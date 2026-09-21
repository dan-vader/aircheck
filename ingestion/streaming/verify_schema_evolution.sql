-- Databricks notebook source
DESCRIBE TABLE dbr_dev_ua5816bd.aircheck_bronze.sensor_live_raw;

-- COMMAND ----------

DESCRIBE TABLE dbr_dev_ua5816bd.aircheck_bronze.sensor_live_raw;

-- COMMAND ----------

SELECT sensor_type, P1, P2, temperature, humidity, pressure, _rescued_data
FROM dbr_dev_ua5816bd.aircheck_bronze.sensor_live_raw
ORDER BY _ingested_at DESC
LIMIT 20;

-- COMMAND ----------

SELECT COUNT(*) AS rescued_count
FROM dbr_dev_ua5816bd.aircheck_bronze.sensor_live_raw
WHERE _rescued_data IS NOT NULL;

-- COMMAND ----------

SELECT COUNT(*) AS rescued_count,
       COUNT(*) * 100.0 / (SELECT COUNT(*) FROM dbr_dev_ua5816bd.aircheck_bronze.sensor_live_raw) AS rescued_pct
FROM dbr_dev_ua5816bd.aircheck_bronze.sensor_live_raw
WHERE _rescued_data IS NOT NULL;

-- COMMAND ----------

SELECT sensor_type,
       COUNT(*) AS rescued_rows
FROM dbr_dev_ua5816bd.aircheck_bronze.sensor_live_raw
WHERE _rescued_data IS NOT NULL
GROUP BY sensor_type
ORDER BY rescued_rows DESC;

-- COMMAND ----------

SELECT get_json_object(_rescued_data, '$') AS raw_extra,
       COUNT(*) AS cnt
FROM dbr_dev_ua5816bd.aircheck_bronze.sensor_live_raw
WHERE _rescued_data IS NOT NULL
GROUP BY get_json_object(_rescued_data, '$')
ORDER BY cnt DESC
LIMIT 15;

-- COMMAND ----------

SELECT COUNT(*) AS total_rows
FROM dbr_dev_ua5816bd.aircheck_bronze.sensor_live_raw;