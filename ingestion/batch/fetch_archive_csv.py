# Databricks notebook source
import os
import re
import yaml
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from datetime import datetime, timedelta

# COMMAND ----------

default_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

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

batch_cfg = config["batch"]
archive_base_url = batch_cfg["base_url"]
file_limit = batch_cfg["file_limit"]

# COMMAND ----------

session = requests.Session()
retry_strategy = Retry(
    total=3,
    backoff_factor=1,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET"],
)
adapter = HTTPAdapter(max_retries=retry_strategy)
session.mount("https://", adapter)
session.mount("http://", adapter)

# COMMAND ----------

days_count = (end_date - start_date).days + 1
if days_count < 1:
    raise ValueError("end_date cannot be before start_date")

for i in range(days_count):
    current_date = (start_date + timedelta(days=i)).strftime("%Y-%m-%d")

    try:
        day_url = f"{archive_base_url}{current_date}/"
        response = session.get(day_url, headers=headers, timeout=30)

        if response.status_code == 404:
            print(f"No archive for {current_date} (404).")
            continue
        response.raise_for_status()

        csv_files = re.findall(r'href="([^"]+\.csv)"', response.text)
        
        if file_limit:
            csv_files = csv_files[:file_limit]

        if not csv_files:
            # No CSV files found for current_date
            continue

        volume_path = f"/Volumes/{catalog_name}/{bronze_schema}/{volume_name}/batch/archive/date={current_date}/"
        os.makedirs(volume_path, exist_ok=True)

        downloaded_count = 0
        skipped_count = 0

        for file_name in csv_files:
            file_url = day_url + file_name
            dest_path = os.path.join(volume_path, file_name)

            if os.path.exists(dest_path) and os.path.getsize(dest_path) > 0:
                skipped_count += 1
                continue

            try:
                with session.get(file_url, headers=headers, stream=True, timeout=30) as file_response:
                    if file_response.status_code == 200:
                        with open(dest_path, "wb") as f:
                            for chunk in file_response.iter_content(chunk_size=8192):
                                if chunk:
                                    f.write(chunk)
                        downloaded_count += 1
                    else:
                        print(f"Failed to download {file_name}: {file_response.status_code}")
                        pass
            except Exception as file_err:
                print(f"Error downloading {file_name}: {file_err}")
                if os.path.exists(dest_path):
                    os.remove(dest_path)
        print(f"Done {current_date}: Downloaded {downloaded_count}, Skipped {skipped_count}.")

    except Exception as e:
        print(f"Error fetching data for {current_date}: {str(e)}")
        raise e