import os
import subprocess
import pandas as pd
from datetime import datetime
from datetime import datetime

# DATE TIME Related VAR
VERSION = datetime.now().strftime("%Y%m%d%H%M%S%f")[:-3]
current_date = datetime.now().strftime("%Y-%m-%d")
CLEANED_DATA_PATH = f"/opt/airflow/dags/data/{current_date}/clean_dataset.csv"
print(VERSION)

# GIT CONFIG
GIT_NAME = "Subhashree Karmakar"
GIT_EMAIL = "user.krmr@gmail.com"
GIT_USERNAME = "userkrmr"
GIT_TOKEN = "github_pat_XXXXXXXXXXXXXXXXXXXXXXXXXXX" # Expires on Sun, May 11 2025.
GIT_REPO_URL = f"https://{GIT_USERNAME}:{GIT_TOKEN}@github.com/{GIT_USERNAME}/dmml-dvc-versioning.git"
LOCAL_REPO_DIR = f"/opt/airflow/dags/git/git_{VERSION}"

# AWS S3 CONFIG
S3_REMOTE = "user-dmml"
S3_BUCKET = "s3://user-dmml/raw-data/dvc-cleaned/"
AWS_ACCESS_KEY = "XXXXXXXXXXXXXXXXXXXXXXXXX"
AWS_SECRET_KEY = "XXXXXXXXXXXXXXXXXXXXXXXXX"
AWS_REGION = "eu-north-1"

# Clone Git repository if not already present
if os.path.exists(LOCAL_REPO_DIR):
    subprocess.run(["rm", "-rf", LOCAL_REPO_DIR])  # Delete non-empty directory to start fresh

print(f"Cloning Git repository: {GIT_REPO_URL}")
subprocess.run(["git", "clone", GIT_REPO_URL, LOCAL_REPO_DIR], check=True)

# Verify that the repo was cloned properly
if not os.path.exists(os.path.join(LOCAL_REPO_DIR, ".git")):
    print("⚠️ Error: .git directory is missing! Re-initializing the repository.")
    os.chdir(LOCAL_REPO_DIR)
    subprocess.run(["git", "init"], check=True)
    subprocess.run(["git", "remote", "add", "origin", GIT_REPO_URL], check=True)
    subprocess.run(["git", "fetch"], check=True)
    subprocess.run(["git", "checkout", "main"], check=True)

# # Data storage
# REPO_DATA_DIR = f"{LOCAL_REPO_DIR}/data/"
# os.makedirs(REPO_DATA_DIR, exist_ok=True)

# # Ensure the data directory exists
# os.makedirs(REPO_DATA_DIR, exist_ok=True)

# Navigate to the repo
os.chdir(LOCAL_REPO_DIR)

# Initialize DVC if not already done
if not os.path.exists(".dvc"):
    print("Initializing DVC...")
    subprocess.run(["dvc", "init", "-f"], check=True)
    subprocess.run(["dvc", "remote", "add", S3_REMOTE, S3_BUCKET], check=True)
    
    # Configure DVC to use stored AWS credentials (no prompt)
    subprocess.run(["dvc", "remote", "modify", S3_REMOTE, "access_key_id", AWS_ACCESS_KEY], check=True)
    subprocess.run(["dvc", "remote", "modify", S3_REMOTE, "secret_access_key", AWS_SECRET_KEY], check=True)
    subprocess.run(["dvc", "remote", "modify", S3_REMOTE, "region", AWS_REGION], check=True)

    subprocess.run(["git", "add", ".dvc", ".dvc/config"], check=True)
    subprocess.run(["git", "commit", "-m", "Initialize DVC with S3"], check=True)
    subprocess.run(["git", "pull", "--rebase"], check=True)
    subprocess.run(["git", "push", "--set-upstream", "origin", "main", "--force"], check=True)

# Create and write to the VERSION file
with open(f"{LOCAL_REPO_DIR}/VERSION", "w") as file:
    file.write(VERSION)

# Process & Save Data (Replace with actual data processing)
data = pd.read_csv(CLEANED_DATA_PATH)
data.to_csv(f"{LOCAL_REPO_DIR}/clean_dataset.csv", index=False)
print(f"Data saved: {f"{LOCAL_REPO_DIR}/clean_dataset.csv"}")

subprocess.run(["git", "config", "--global", "user.email", GIT_EMAIL], check=True)
subprocess.run(["git", "config", "--global", "user.name", GIT_NAME], check=True)

# Track new data with DVC
subprocess.run(["dvc", "add", f"{LOCAL_REPO_DIR}/clean_dataset.csv"], check=True)
subprocess.run(["git", "add", "."], check=True)
subprocess.run(["git", "commit", "-m", f"Versioned dataset {VERSION}"], check=True)

# Push data to S3 (no credential prompt)
subprocess.run(["dvc", "push", "-r", S3_REMOTE], check=True)

# Push metadata to GitHub
subprocess.run(["git", "push"], check=True)

print("Data versioned and stored in S3!")
