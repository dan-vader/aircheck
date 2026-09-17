# Databricks notebook source
# MAGIC %md
# MAGIC # Least-privilege grants for _aircheck_ schemas and volume

# COMMAND ----------

import yaml

cfg = yaml.safe_load(open("../config/aircheck.yaml"))["dev"]

try:
    team = yaml.safe_load(open("../config/team.local.yaml"))  # gitignored
except FileNotFoundError:
    raise FileNotFoundError(
        "config/team.local.yaml not found. "
        "Copy config/team.example.yaml to config/team.local.yaml "
        "and fill in the actual team emails."
    )

catalog = cfg["catalog"]
schemas = cfg["schemas"]
volume = f"{catalog}.{schemas['bronze']}.{cfg['volume']}"

# COMMAND ----------

for email in team.values():
    for layer in ("bronze", "silver", "gold", "ops"):
        spark.sql(f"GRANT USE SCHEMA, SELECT ON SCHEMA {catalog}.{schemas[layer]} TO `{email}`")
    spark.sql(f"GRANT READ VOLUME ON VOLUME {volume} TO `{email}`")

# COMMAND ----------

for role in ("batch", "stream"):
    e = team[role]
    spark.sql(f"GRANT CREATE TABLE, MODIFY ON SCHEMA {catalog}.{schemas['bronze']} TO `{e}`")
    spark.sql(f"GRANT WRITE VOLUME ON VOLUME {volume} TO `{e}`")
    spark.sql(f"GRANT CREATE TABLE, MODIFY ON SCHEMA {catalog}.{schemas['ops']} TO `{e}`")

# COMMAND ----------

for layer in ("silver", "gold", "ops"):
    spark.sql(f"GRANT CREATE TABLE, MODIFY ON SCHEMA {catalog}.{schemas[layer]} TO `{team['analytics']}`")

# COMMAND ----------

for layer in ("bronze", "silver", "gold", "ops"):
    display(spark.sql(f"SHOW GRANTS ON SCHEMA {catalog}.{schemas[layer]}"))