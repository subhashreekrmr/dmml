import os
import boto3
import pandas as pd
from datetime import datetime

# AWS S3 Configuration
S3_BUCKET_NAME = "user-dmml" #"telco-churn-data1"  # Replace with your bucket name
S3_FOLDER = "raw-data/"  # S3 prefix (folder-like structure)

# Define local paths
DATA_DIR = "/opt/airflow/dags/data"
FILE_NAME = "telco_customer_churn.csv"
FILE_PATH = os.path.join(DATA_DIR, datetime.today().strftime('%Y-%m-%d'), FILE_NAME)


# Create an S3 client s3uri - s3://dmml-assignment/raw-data/
s3_client = boto3.client("s3",
                         aws_access_key_id='AKIIPQSK7Q',
                         aws_secret_access_key='Zde/o2+72tG4K09C8Y2EbUaVfN',
                         region_name='eu-north-1'
                         )

def upload_to_s3(local_file, bucket, s3_folder):
    print(local_file)
    """Uploads a file to AWS S3 with a timestamped key."""
    if os.path.exists(local_file):
        today_date = datetime.today().strftime('%Y-%m-%d')
        s3_key = f"raw-data/{today_date}/telco_customer_churn.csv"
        
        try:
            s3_client.upload_file(
                Filename=local_file,
                Bucket=bucket,
                Key=s3_key 
            )
            # s3_client.upload_file(local_file, bucket, s3_key)
            print(f"✅ Data uploaded to S3: s3://{bucket}/{s3_key}")
        except Exception as e:
            print(f"❌ S3 Upload Error: {e}")
    else:
        print("❌ No ingested data found to upload.")

# Run upload function
# upload_to_s3(RAW_DATA_DIR + datetime.today().strftime('%Y-%m-%d') + FILE_NAME , S3_BUCKET_NAME, S3_FOLDER)

upload_to_s3(FILE_PATH, S3_BUCKET_NAME, S3_FOLDER)