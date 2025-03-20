import pandas as pd
import psycopg2
from sqlalchemy import create_engine
from sqlalchemy import Date as sql_date
from datetime import datetime, date
 
# Database Configuration
DB_HOST = "120.2.30.181"
DB_PORT = "5432"
DB_NAME = "postgres"
DB_USER = "postgres"
DB_PASSWORD = "dmml-hahaha"
TABLE_NAME = "clean_data"
TABLE_NAME2 = "clean_data_new_feature"
 
# Get today's date
current_date = datetime.today().strftime('%Y-%m-%d')

# Load cleaned dataset
cleaned_data_path = f"/opt/airflow/dags/data/{current_date}/clean_dataset.csv"
print(cleaned_data_path)
data = pd.read_csv(cleaned_data_path)
data['modified_date'] = pd.to_datetime(date.today())
data2=data.copy()
data2['all_charges']=data['monthlycharges']*data['tenure']

# Connect to PostgreSQL and insert data
def insert_to_db(df,TABLE_NAME):
    try:
        # Create connection string
        connection_string = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        print(connection_string)
        engine = create_engine(connection_string)
        # Insert data into PostgreSQL table
        df.to_sql(TABLE_NAME, engine, if_exists="replace", index=False)
        print(f"✅ Data successfully inserted into {TABLE_NAME}.")
    except Exception as e:
        print(f"❌ Error inserting data: {e}")
        raise


insert_to_db(data,TABLE_NAME)
insert_to_db(data2,TABLE_NAME2)