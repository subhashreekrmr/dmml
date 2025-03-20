import pandas as pd
from sklearn.model_selection import train_test_split, GridSearchCV, StratifiedKFold
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.neighbors import KNeighborsClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler
from imblearn.over_sampling import SMOTE
from sklearn.metrics import (accuracy_score, classification_report,
                             roc_auc_score, f1_score, precision_score, recall_score)
import warnings
import joblib
import psycopg2
from datetime import datetime
import os
import mlflow

# Ignore warnings
warnings.filterwarnings("ignore")

# PostgreSQL Database Configuration
DB_HOST = "120.2.30.181"
DB_PORT = "5432"
DB_NAME = "postgres"
DB_USER = "postgres"
DB_PASSWORD = "dmml-hahaha"
TABLE_NAME = "clean_data_new_feature"

BASE_DIR = "/opt/airflow/dags/data"
today_date = datetime.today().strftime('%Y-%m-%d')
FILE_DIR = os.path.join(BASE_DIR, today_date)
os.makedirs(FILE_DIR, exist_ok=True)
FILE_NAME = "performance_report.csv"
FILE_PATH = os.path.join(FILE_DIR, FILE_NAME)

# Connect to PostgreSQL Database
try:
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )
    print("✅ Successfully connected to PostgreSQL")

    # Load data from PostgreSQL
    query = f"SELECT * FROM {TABLE_NAME}"
    data = pd.read_sql(query, conn)
    print(f"✅ Loaded {len(data)} records from {TABLE_NAME}")

except Exception as e:
    print("❌ Error connecting to PostgreSQL:", e)

finally:
    if conn:
        conn.close()
        print("🔌 PostgreSQL connection closed")

data = data.dropna()

scaler = StandardScaler()

data[['all_charges']] = scaler.fit_transform(data[['all_charges']])

# Feature Selection
X = data.drop(columns=['churn','modified_date','monthlycharges'])
y = data['churn']

# Handle class imbalance using SMOTE
smote = SMOTE(sampling_strategy=0.5, random_state=42)
X_resampled, y_resampled = smote.fit_resample(X, y)

# Splitting dataset
X_train, X_test, y_train, y_test = train_test_split(X_resampled, y_resampled, test_size=0.2, random_state=42)

# Scale features
scaler = StandardScaler()
X_train_resampled = scaler.fit_transform(X_train)
X_test = scaler.transform(X_test)

# Define models and hyperparameter grids
models = {
    'RandomForest': (RandomForestClassifier(), {
        'n_estimators': [100, 200], 
        'max_depth': [10, 20],
        'min_samples_split': [2, 5, 10], 
        'min_samples_leaf': [1, 2],
        'class_weight': ['balanced']
    }),
    'GradientBoosting': (GradientBoostingClassifier(), {
        'n_estimators': [50, 100],
        'learning_rate': [0.1, 0.2],
        'max_depth': [3, 5, 10]
    }),
    'KNN': (KNeighborsClassifier(), {
        'n_neighbors': [3, 5, 7, 9], 
        'weights': ['uniform', 'distance'],
        'metric': ['euclidean', 'manhattan', 'minkowski']
    }),
    'LogisticRegression': (LogisticRegression(), {
        'C': [0.01, 0.1, 1, 10, 100],
        'solver': ['liblinear', 'saga', 'lbfgs'],
        'max_iter': [100, 200, 500],
        'class_weight': ['balanced'],
        'penalty': ['l1', 'l2']
    })
}


# Perform Grid Search for each model
best_models = {}
skf = StratifiedKFold(n_splits=5)
best_model = None
best_score = -float("inf")

mlflow.set_tracking_uri("http://172.25.20.135:5000")
mlflow.autolog()

for model_name, (model, param_grid) in models.items():
    print(f"Training {model_name}...")
        
    grid_search = GridSearchCV(model, param_grid, cv=skf, scoring='roc_auc')
    grid_search.fit(X_train_resampled, y_train)
    
    best_models[model_name] = grid_search.best_estimator_
    print(f"Best parameters for {model_name}: {grid_search.best_params_}")

    if grid_search.best_score_ > best_score:
        best_score = grid_search.best_score_
        best_model = grid_search.best_estimator_
        best_model_name = model_name

# Evaluate each best model on the test set
result = []
for model_name, best_model in best_models.items():
    y_pred = best_model.predict(X_test)
    print(f"\nModel: {model_name}")
    print("Accuracy:", accuracy_score(y_test, y_pred))
    print("F1 Score:", f1_score(y_test, y_pred))
    print("ROC AUC:", roc_auc_score(y_test, y_pred))
    print("Precision:", precision_score(y_test, y_pred))
    print("Recall:", recall_score(y_test, y_pred))
    print("Classification Report:\n", classification_report(y_test, y_pred))
    result.append((model_name, accuracy_score(y_test, y_pred), roc_auc_score(y_test, y_pred), f1_score(y_test, y_pred), precision_score(y_test, y_pred), recall_score(y_test, y_pred)))

# Save the best model
joblib.dump(best_model, f"best_model_{best_model_name}.pkl")
print(f"Best model: {best_model_name} with accuracy: {best_score}")

# Define column names
columns = ["Model", "Accuracy", "ROC AUC", "F1 Score", "Precision", "Recall"]

# Convert result list to DataFrame
df_results = pd.DataFrame(result, columns=columns)

# Save to CSV
df_results.to_csv(FILE_PATH, index=False, mode="w")