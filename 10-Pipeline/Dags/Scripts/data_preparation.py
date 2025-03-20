# Import required library
import pandas as pd
import warnings
import boto3
from sklearn.preprocessing import StandardScaler, LabelEncoder
from datetime import datetime
import io

#Ignore warnings
warnings.filterwarnings("ignore")

# Get today's date in YYYY-MM-DD format
current_date = datetime.today().strftime('%Y-%m-%d')

# AWS S3 Configurations
BUCKET_NAME = "user-dmml"
FILE_KEY = f"raw-data/{current_date}/telco_customer_churn.csv"  # Adjust the path inside the bucket


# Initialize S3 Client
s3 = boto3.client("s3",
                  aws_access_key_id='XXXXXXXXXXXXXXXXXXXX',
                  aws_secret_access_key='XXXXXXXXXXXXXXXXXXXX',
                  region_name='eu-north-1')

# Download Data from S3
response = s3.get_object(Bucket=BUCKET_NAME, Key=FILE_KEY)
data = pd.read_csv(io.BytesIO(response["Body"].read()))


#DATA PREPROCESSING

# 1. Missing Values 
# Identify categorical and numerical columns
categorical_cols1 = ['gender', 'seniorcitizen', 'partner', 'dependents', 'phoneservice', 'multiplelines', 
                     'internetservice', 'onlinesecurity', 'onlinebackup', 'deviceprotection', 'techsupport',
                     'streamingtv', 'streamingmovies', 'contract', 'paperlessbilling', 'paymentmethod', 'churn']

numeric_cols1 = ['tenure', 'monthlycharges', 'customerid', 'totalcharges']

# Convert column names to lowercase to match the actual column names in the dataframe
data.columns = data.columns.str.lower()

# Convert categorical column names to lowercase to match the dataframe
categorical_cols1 = [col.lower() for col in categorical_cols1]

# Replace missing values in categorical columns with "No"
data[categorical_cols1] = data[categorical_cols1].fillna("No")

# Handle binary (0/1) columns separately
binary_cols = [col for col in categorical_cols1 if data[col].dropna().isin([0, 1]).all()]

# Fill missing values in binary columns with 0 (assuming 0 = No)
data[binary_cols] = data[binary_cols].fillna(0)

# Drop rows with missing values in other numerical columns
data = data.dropna(subset=[col for col in numeric_cols1 if col not in binary_cols])

# Print missing values count after handling
print("Missing values after handling:\n", data.isnull().sum())

# 2. Rectifying data-types
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
for col, expected_type in expected_types.items():
    actual_type = str(data[col].dtype)
    if actual_type != expected_type:
        if expected_type == "int64":
            data[col] = pd.to_numeric(data[col], errors="coerce").astype("Int64")
            print(f"Column {col} is converted into int64 data-type")
        elif expected_type == "float64":
            data[col] = pd.to_numeric(data[col], errors="coerce")
            print(f"Column {col} is converted into float64 data-type")
        elif expected_type == "object":
            print(f"Column {col} is converted object int64 data-type")
            data[col] = data[col].astype(str) 
# Ensures that each column in the dataset has the correct data type as defined in the expected_types dictionary.

# 3. Removing duplicate values
data = data.drop_duplicates(subset="customerid", keep="first")

# 4. Convert all categorical values to lowercase
categorical_cols2 = cols = ['gender', 'partner', 'dependents', 'phoneservice', 'multiplelines', 'internetservice', 
        'onlinesecurity', 'onlinebackup', 'deviceprotection', 'techsupport', 'streamingtv', 
        'streamingmovies', 'contract', 'paperlessbilling', 'paymentmethod', 'churn']

data[categorical_cols2] = data[categorical_cols2].apply(lambda col: col.str.lower())
# Converts all values in the specified categorical columns to lowercase. It ensures consistency in text formatting, making the data uniform and reducing the risk of mismatches due to case differences.

# 5. Handling Range value
data["tenure"] = data["tenure"].clip(lower=0, upper=100)
# Ensures that values in the "tenure" column are within the range of 0 to 100

# Drop customerid column
data.drop(columns='customerid', inplace=True)

# FEATURE ENGINNERING
# Label Encoding for binary categorical columns
binary_cols = ['gender', 'partner', 'dependents', 'phoneservice', 'paperlessbilling', 'churn', 'internetservice', 'contract', 'paymentmethod', 'multiplelines', 'onlinesecurity', 
        'onlinebackup', 'deviceprotection', 'techsupport', 'streamingtv', 'streamingmovies']
le = LabelEncoder()
for col in binary_cols:
    data[col] = le.fit_transform(data[col])

# Standardizing numerical columns
scaler = StandardScaler()
data[['totalcharges']] = scaler.fit_transform(data[['totalcharges']])

# to change path
data.to_csv(f"/opt/airflow/dags/data/{current_date}/clean_dataset.csv", index=False, mode="w")

