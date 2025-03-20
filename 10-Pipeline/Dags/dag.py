from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import runpy
 
def data_ing():
    runpy.run_path('/opt/airflow/dags/data_ingestion.py')

def raw_data():
    runpy.run_path('/opt/airflow/dags/raw_data_storage.py')
 
def aws_s3_upload():
    runpy.run_path('/opt/airflow/dags/raw_data_storage.py')

def data_validation():
    runpy.run_path('/opt/airflow/dags/data_validation.py')

def data_preparation():
    runpy.run_path('/opt/airflow/dags/data_preparation.py')

def data_transformation_storage():
    runpy.run_path('/opt/airflow/dags/data_transformation_storage.py')

def dvc_data_versioning():
    runpy.run_path('/opt/airflow/dags/dvc_versioning.py')

def building_model():
    runpy.run_path('/opt/airflow/dags/model_building.py')

def read_files():
    runpy.run_path('/opt/airflow/dags/read_files.py')
 
def transform_data():
    return "Transform Code"
 
def load_data():
    return "Load Data"
 
default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 3, 11),
    'retries': 0,  # Disable retries
    'depends_on_past': False,
}
 
# dag = DAG(
#     dag_id='test_222',
#     default_args=default_args,
#     schedule_interval="@daily"
#     #schedule_interval="*/30 * * * *"
# )

# Define the DAG with manual trigger only
dag = DAG(
    dag_id="test_222",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
)
 
data_ingestion = PythonOperator(
    task_id="data_ingestion",
    python_callable=data_ing,
    dag=dag
)


raw_data_storage = PythonOperator(
    task_id="raw_data_storage",
    python_callable=raw_data,
    dag=dag
)
 
s3_upload = PythonOperator(
   task_id="s3_upload",
     python_callable=aws_s3_upload,
    dag=dag
)
 
file_read = PythonOperator(
    task_id="file_read",
    python_callable=read_files,
    dag=dag
)

merged_data_validation = PythonOperator(
     task_id="data_validation",
     python_callable=data_validation,
     dag=dag
 )

merged_data_preparation = PythonOperator(
     task_id="data_preparation",
     python_callable=data_preparation,
     dag=dag
 )
 
merged_data_transformation_storage = PythonOperator(
     task_id="data_transformation_storage",
     python_callable=data_transformation_storage,
     dag=dag
 )
 
data_versioning = PythonOperator(
     task_id="data_versioning",
     python_callable=dvc_data_versioning,
     dag=dag
 )
 
model_building = PythonOperator(
     task_id="model_building",
     python_callable=building_model,
     dag=dag
 )
 
# Define task dependencies
# data_ingestion >> raw_data_storage >> s3_upload >> file_read >> merged_data_validation >> merged_data_preparation >> merged_data_transformation_storage >> data_versioning
 
data_ingestion >> raw_data_storage >> s3_upload >> file_read >> merged_data_validation >> merged_data_preparation >> merged_data_transformation_storage >> data_versioning >> model_building
 