import requests
import re
import os
import yaml
from datetime import datetime, timedelta
# TODO: add logging


dbutils.widgets.dropdown("env", "dev", ["dev", "prod", "test"], "Environment")
default_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")   # Default date: Yesterday
dbutils.widgets.text("date", default_date, "Date (YYYY-MM-DD)")

env = dbutils.widgets.get("env")
date = dbutils.widgets.get("date")


CONFIG_PATH = "../../config/aircheck.yaml"
with open(CONFIG_PATH, "r") as f:
    full_config = yaml.safe_load(f)

config = full_config[env]
catalog_name = config["catalog"]
bronze_schema = config["schemas"]["bronze"]
volume_name = config["volume"]
secret_scope = config["secret_scope"]
contact_email_key = config["secrets"]["contact_email"]


try:
    contact_email = dbutils.secrets.get(secret_scope, contact_email_key)
    headers = {"User-Agent": f"databricks-lab-team (contact: {contact_email})"}

    base_url = f"https://archive.sensor.community/{date}/"
    response = requests.get(base_url, headers=headers, timeout=30)
    response.raise_for_status()

    csv_files = re.findall(r'href="([^"]+\.csv)"', response.text)
    csv_files = csv_files[:30]  # temporary limit

    if not csv_files:
        # logging
        pass
    else:
        volume_path = f"/Volumes/{catalog_name}/{bronze_schema}/{volume_name}/batch/archive/date={date}/"
        os.makedirs(volume_path, exist_ok=True)

        downloaded_count = 0

        for file_name in csv_files:
            file_url = base_url + file_name
            dest_path = os.path.join(volume_path, file_name)

            file_response = requests.get(file_url, headers=headers, stream=True, timeout=30)
            if file_response.status_code == 200:
                with open(dest_path, "wb") as f:
                    for chunk in file_response.iter_content(chunk_size=8192):
                        f.write(chunk)
                downloaded_count += 1
            else:
                # logging
                pass
        # logging
    
except Exception as e:
    # logging
    print(f"Error in fetch: {str(e)}")
    raise e
