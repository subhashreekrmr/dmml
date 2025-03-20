import os
import requests
import pandas as pd
import psycopg2
from sqlalchemy import create_engine

# Define dataset URL (direct Kaggle download won't work; use manual download or Kaggle API)
DATASET_URL = "https://raw.githubusercontent.com/userkrmr/dmml/refs/heads/main/telco_churn_kaggle.csv"  # Alternative source
RAW_DATA_DIR = "/opt/airflow/dags/data/raw"
FILE_NAME = "telco_customer_churn.csv"
DB_HOST = "120.2.30.181"
DB_PORT = "5432"
DB_NAME = "postgres"
DB_USER = "postgres"
DB_PASSWORD = "dmml-hahaha"
# TABLE_NAME = "telco_churn"

# Ensure the directory exists
os.makedirs(RAW_DATA_DIR, exist_ok=True)
FILE_PATH = os.path.join(RAW_DATA_DIR, FILE_NAME)

def download_data(url, file_path):
    """Download dataset from a given URL and save it locally."""
    response = requests.get(url)
    
    if response.status_code == 200:
        with open(file_path, "wb") as file:
            file.write(response.content)
        print(f"✅ Data successfully downloaded and saved to {file_path}")
    else:
        print(f"❌ Failed to download data. Status code: {response.status_code}")

def load_data_csv(file_path):
    """Load CSV file into a Pandas DataFrame."""
    try:
        df = pd.read_csv(file_path)
        print(f"✅ Data successfully loaded. Shape: {df.shape}")
        return df
    except Exception as e:
        print(f"❌ Error loading data: {e}")
        return None

def load_data_postgres(db_user, db_password, db_host, db_name, db_port=5432):
    try:
        engine = create_engine(f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}")
        # Load data from PostgreSQL into Pandas DataFrame
        query = "SELECT * FROM telco_churn;"  # Change table name accordingly
        df_sql = pd.read_sql(query, con=engine)
        print(f"✅ PostgreSQL Data Loaded: {df_sql.shape}")
        return df_sql
    except Exception as e:
        print(f"❌ Error loading data: {e}")
        return None

# Run ingestion
if os.path.exists(FILE_PATH):
    os.remove(FILE_PATH)

download_data(DATASET_URL, FILE_PATH)
# Load data into DataFrame
df_csv = load_data_csv(FILE_PATH)
print(df_csv)
df_csv.rename(columns={'customerID': 'customerid'}, inplace=True)

df_postgres = load_data_postgres(DB_USER, DB_PASSWORD, DB_HOST, DB_NAME, DB_PORT)
print(df_postgres)

# **Merge CSV and PostgreSQL Data on `customerID`**
merged_df = pd.merge(df_csv, df_postgres, on="customerid", how="inner")
print(merged_df)
# merged_df=df_csv["customerid"].str.lower(), right_on=df_postgres["customerid"].str.lower()

print(f"✅ Merged Data Shape: {merged_df.shape}")

# Save merged data
merged_df.to_csv(FILE_PATH, index=False)
print("✅ Merged data saved as '" + str(FILE_PATH) + "'")

# Display first few rows
if merged_df is not None:
    print(merged_df.head())
