import os
import shutil
import pandas as pd
from datetime import datetime

# Define storage paths
BASE_DIR = "/opt/airflow/dags/data"
FILE_NAME = "telco_customer_churn.csv"
LOG_FILE = "ingestion_log.txt"

# Create folder for today's date
today_date = datetime.today().strftime('%Y-%m-%d')
STORAGE_PATH = os.path.join(BASE_DIR, today_date)
os.makedirs(STORAGE_PATH, exist_ok=True)

RAW_FILE_PATH = os.path.join("/opt/airflow/dags/data/raw", FILE_NAME)
STORED_FILE_PATH = os.path.join(STORAGE_PATH, f"telco_customer_churn.csv")
LOG_FILE_PATH = os.path.join(STORAGE_PATH, LOG_FILE)


def store_raw_data():
    """Move the ingested data to the date-partitioned raw storage."""
    if os.path.exists(RAW_FILE_PATH):
        shutil.copy(RAW_FILE_PATH, STORED_FILE_PATH)
        with open(LOG_FILE_PATH, "a") as log:
            log.write(f"{datetime.now()} -  Data stored at {STORED_FILE_PATH}\n")
        print(f" Data stored in {STORED_FILE_PATH}")
    else:
        print(" No ingested data found to store.")

# Run storage function
store_raw_data()
