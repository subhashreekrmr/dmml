import pandas as pd
import boto3
import io
from datetime import datetime

# Get today's date in YYYY-MM-DD format
current_date = datetime.today().strftime('%Y-%m-%d')

# AWS S3 Configurations
BUCKET_NAME = "user-dmml"
FILE_KEY = f"raw-data/{current_date}/telco_customer_churn.csv"  # Adjust the path inside the bucket
# FILE_KEY = "raw/2025-03-09/telco_customer_churn.csv"


# Initialize S3 Client
s3 = boto3.client("s3",
                  aws_access_key_id='AKIAQSK7Q',
                  aws_secret_access_key='ZdePu09C8Y2EbUaVfN',
                  region_name='eu-north-1')

# Download Data from S3
response = s3.get_object(Bucket=BUCKET_NAME, Key=FILE_KEY)
data = pd.read_csv(io.BytesIO(response["Body"].read()))

#-----------------------------------------------------------#
# Creating Validation Report
validation_report = pd.DataFrame(columns=['Issue', 'Resolution'])

# Check for missing values
null_columns = data.columns[data.isnull().any()].tolist()
if null_columns:
    new_data = {
        "Issue": f"Columns with missing values: {null_columns}",
        "Resolution": "If categorical, replace with No/0; else drop missing rows"
    }
    validation_report = pd.concat([validation_report, pd.DataFrame([new_data])], ignore_index=True)
    
# Expected Data Types
expected_types = {
    "customerid": "object",
    "gender": "object",
    "seniorcitizen": "int64",
    "partner": "object",
    "dependents": "object",
    "tenure": "int64",
    "phoneservice": "object",
    "multiplelines": "object",
    "internetservice": "object",
    "onlinesecurity": "object",
    "onlinebackup": "object",
    "deviceprotection": "object",
    "techsupport": "object",
    "streamingtv": "object",
    "streamingmovies": "object",
    "contract": "object",
    "paperlessbilling": "object",
    "paymentmethod": "object",
    "monthlycharges": "float64",
    "totalcharges": "float64",
    "churn": "object"
}

# Check for incorrect data types
for col, expected_type in expected_types.items():
    actual_type = str(data[col].dtype)
    if actual_type != expected_type:
        new_data = {
            "Issue": f"Column '{col}' has type {actual_type}, expected {expected_type}",
            "Resolution": f"Convert column '{col}' to {expected_type} datatype"
        }
        validation_report = pd.concat([validation_report, pd.DataFrame([new_data])], ignore_index=True)

# Check for duplicate values
if data.duplicated(subset=["customerid"]).sum() > 0:
    new_data = {
        "Issue": "Duplicate customer IDs present",
        "Resolution": "Remove duplicate rows"
    }
    validation_report = pd.concat([validation_report, pd.DataFrame([new_data])], ignore_index=True)

# Check for inconsistent capitalization in categorical columns
categorical_cols = [
    'gender', 'seniorcitizen', 'partner', 'dependents', 'phoneservice', 'multiplelines', 
    'internetservice', 'onlinesecurity', 'onlinebackup', 'deviceprotection', 'techsupport',
    'streamingtv', 'streamingmovies', 'contract', 'paperlessbilling', 'paymentmethod', 'churn'
]


data[categorical_cols] = data[categorical_cols].astype(str)

columns_with_case_issues = [
    col for col in categorical_cols if data[col].str.lower().nunique() != data[col].nunique()
]
if columns_with_case_issues:
    new_data = {
        "Issue": f"Columns with inconsistent capitalization: {columns_with_case_issues}",
        "Resolution": "Convert categorical values to lowercase"
    }
    validation_report = pd.concat([validation_report, pd.DataFrame([new_data])], ignore_index=True)

# Check for invalid tenure range
if not data['tenure'].between(0, 100).all():
    invalid_tenure = data[(data['tenure'] < 0) | (data['tenure'] > 100)]
    new_data = {
        "Issue": f"Invalid Tenure values found",
        "Resolution": "Clip values to a valid range (0-100)"
    }
    validation_report = pd.concat([validation_report, pd.DataFrame([new_data])], ignore_index=True)

# Check for outliers in numerical columns
numeric_cols = ['tenure', 'monthlycharges']
for col in numeric_cols:
    Q1 = data[col].quantile(0.25)
    Q3 = data[col].quantile(0.75)
    IQR = Q3 - Q1
    lower_bound = Q1 - 1.5 * IQR
    upper_bound = Q3 + 1.5 * IQR

    outliers = data[(data[col] < lower_bound) | (data[col] > upper_bound)]
    if not outliers.empty:
        new_data = {
            "Issue": f"Outliers in {col}: {len(outliers)}",
            "Resolution": "Consider removing outliers or applying transformations"
        }
        validation_report = pd.concat([validation_report, pd.DataFrame([new_data])], ignore_index=True)

# Save Validation Report
validation_report.to_csv("/opt/airflow/dags/data/raw/validation_report.csv", mode="w", index=False)

print("✅ Validation report saved as /opt/airflow/dags/data/raw/validation_report.csv")
