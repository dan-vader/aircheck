# Databricks notebook source
import requests
import re
import os
import yaml
from datetime import datetime, timedelta

# COMMAND ----------

default_date = (datetime.now() - timedelta(days=1)).strftime(
    "%Y-%m-%d"
)  # Default date: Yesterday

dbutils.widgets.text("start_date", default_date, "1. Start date (YYYY-MM-DD)")
dbutils.widgets.text("end_date", default_date, "2. End date (YYYY-MM-DD)")
dbutils.widgets.text("env", "dev", "3. Environment")

start_date = datetime.strptime(dbutils.widgets.get("start_date"), "%Y-%m-%d")
end_date = datetime.strptime(dbutils.widgets.get("end_date"), "%Y-%m-%d")
env = dbutils.widgets.get("env")

# COMMAND ----------

CONFIG_PATH = "../../config/aircheck.yaml"
with open(CONFIG_PATH, "r") as f:
    full_config = yaml.safe_load(f)

config = full_config[env]
catalog_name = config["catalog"]
bronze_schema = config["schemas"]["bronze"]
volume_name = config["volume"]
secret_scope = config["secret_scope"]
contact_email_key = config["secrets"]["contact_email"]

contact_email = dbutils.secrets.get(secret_scope, contact_email_key)
headers = {"User-Agent": f"databricks-lab-team (contact: {contact_email})"}

# COMMAND ----------

days_count = (end_date - start_date).days + 1
if days_count < 1:
    raise ValueError("end_date cannot be before start_date")

for i in range(days_count):
    current_date = (start_date + timedelta(days=i)).strftime("%Y-%m-%d")

    try:
        base_url = f"https://archive.sensor.community/{current_date}/"
        response = requests.get(base_url, headers=headers, timeout=30)

        if response.status_code == 404:
            # Not found for {current_date}
            continue
        response.raise_for_status()

        csv_files = re.findall(r'href="([^"]+\.csv)"', response.text)
        csv_files = csv_files[:30]  # temporary limit

        if not csv_files:
            # No CSV files found for current_date
            continue

        volume_path = f"/Volumes/{catalog_name}/{bronze_schema}/{volume_name}/batch/archive/date={current_date}/"
        os.makedirs(volume_path, exist_ok=True)

        downloaded_count = 0

        for file_name in csv_files:
            file_url = base_url + file_name
            dest_path = os.path.join(volume_path, file_name)

            file_response = requests.get(
                file_url, headers=headers, stream=True, timeout=30
            )
            if file_response.status_code == 200:
                with open(dest_path, "wb") as f:
                    for chunk in file_response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                downloaded_count += 1
            else:
                # Failed to download file_name: responce.status_code
                pass
            # logging

    except Exception as e:
        # logging
        print(f"Error fetching data for {current_date}: {str(e)}")
        raise e