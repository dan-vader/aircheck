# Databricks notebook source
import os
import re
import time
import yaml
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from concurrent.futures import ThreadPoolExecutor, as_completed
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

MAX_WORKERS = batch_cfg["max_workers"]
REQ_TIMEOUT = batch_cfg["request_timeout"]

registry_cfg = batch_cfg["registry"]
registry_table_name = batch_cfg["registry_table"]
registry_table = f"{catalog_name}.{bronze_schema}.{registry_table_name}"
ALLOWED_SENSORS = registry_cfg["allowed_sensors"]
allowed_sensors_lower = [s.lower() for s in ALLOWED_SENSORS]

# COMMAND ----------

session = requests.Session()
retry_strategy = Retry(
    total=3,
    backoff_factor=1,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET"],
)
adapter = HTTPAdapter(max_retries=retry_strategy, pool_connections=MAX_WORKERS, pool_maxsize=MAX_WORKERS)
session.mount("https://", adapter)
session.mount("http://", adapter)

# COMMAND ----------

try:
    known_sensor_ids = {
        str(row["sensor_id"]) for row in
        spark.table(registry_table).select("sensor_id").distinct().collect()
    }
except Exception as e:
    raise ValueError(f"Failed to read from {registry_table}. Ensure build_device_registry has been run. Error: {e}")

if not known_sensor_ids:
    raise ValueError(f"No sensors found in {registry_table}")

# COMMAND ----------

def download_single_file(file_name, day_url, volume_path, max_attempts=3):
    dest_path = os.path.join(volume_path, file_name)

    if os.path.exists(dest_path) and os.path.getsize(dest_path) > 0:
        return "skipped"

    file_url = f"{day_url}{file_name}"

    for attempt in range(1, max_attempts + 1):
        try:
            with session.get(file_url, headers=headers, stream=True, timeout=REQ_TIMEOUT) as file_response:
                if file_response.status_code == 200:
                    with open(dest_path, "wb") as file_handle:
                        for chunk in file_response.iter_content(chunk_size=16384):
                            if chunk:
                                file_handle.write(chunk)
                    return "downloaded"
                elif file_response.status_code == 404:
                    return "failed_404"
                else:
                    last_status = file_response.status_code
        except Exception as file_err:
            if os.path.exists(dest_path):
                os.remove(dest_path)
            if attempt == max_attempts:
                return f"error: {str(file_err)}"
            time.sleep(0.5 * attempt)
            
    return f"failed_{last_status}"

# COMMAND ----------

days_count = (end_date - start_date).days + 1
if days_count < 1:
    raise ValueError("end_date cannot be before start_date")

for i in range(days_count):
    current_date = (start_date + timedelta(days=i)).strftime("%Y-%m-%d")
    day_url = f"{archive_base_url}{current_date}/"

    try:
        response = session.get(day_url, headers=headers, timeout=REQ_TIMEOUT)
        if response.status_code == 404:
            print(f"No archive for {current_date} (404).")
            continue
        response.raise_for_status()

        csv_files = re.findall(r'href="([^"]+\.csv)"', response.text)
        csv_files = [
            f for f in csv_files 
            if any(f"_{s_type}_" in f.lower() for s_type in allowed_sensors_lower)
        ]

        filtered_files = []
        for f in csv_files:
            match = re.search(r'_sensor_(\d+)\.csv', f)
            if match and match.group(1) in known_sensor_ids:
                filtered_files.append(f)

        csv_files = filtered_files
        
        if file_limit:
            csv_files = csv_files[:file_limit]

        if not csv_files:
            print(f"No matching files for {current_date} with filters '{ALLOWED_SENSORS}'.")
            continue

        volume_path = f"/Volumes/{catalog_name}/{bronze_schema}/{volume_name}/batch/archive/date={current_date}/"
        os.makedirs(volume_path, exist_ok=True)

        downloaded_count = 0
        skipped_count = 0
        error_count = 0

        print(f"Starting parallel download of {len(csv_files)} files for {current_date} ...")

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_file = {
                executor.submit(download_single_file, fname, day_url, volume_path): fname 
                for fname in csv_files
            }

            for future in as_completed(future_to_file):
                res = future.result()
                if res == "downloaded":
                    downloaded_count += 1
                elif res == "skipped":
                    skipped_count += 1
                else:
                    error_count += 1
                    file_name = future_to_file[future]
                    print(f"Issue with {file_name}: {res}")
            
        print(f"Done {current_date}: Downloaded {downloaded_count}, Skipped {skipped_count}, Errors {error_count}.")

    except Exception as e:
        print(f"Error fetching data for {current_date}: {str(e)}")
        raise e