from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine
import numpy as np

def extract(**kwargs):
    file_path = '/mnt/d/JN VS/ETL_PIPELINE/covid_data.csv'
    try:
        # Load the CSV file
        df = pd.read_csv(file_path)
        print("Extracted Data Preview:")
        print(df.head())
        kwargs['ti'].xcom_push(key='raw_data', value=df.to_json())
    except Exception as e:
        raise ValueError(f"Error loading file: {e}")

def transform(**kwargs):
    raw_data = kwargs['ti'].xcom_pull(key='raw_data', task_ids='extract')
    if raw_data is None:
        raise ValueError("No data found from extract task")

    df = pd.read_json(raw_data)

    # Replace placeholder values (97) and empty strings with NaN
    df.replace({97: np.nan, '': np.nan}, inplace=True)

    # Convert DATE_DIED from timestamp (milliseconds) or NaN to proper datetime
    df['DATE_DIED'] = pd.to_datetime(df['DATE_DIED'], unit='ms', errors='coerce')

    # Ensure columns like SEX, PATIENT_TYPE are integers
    int_cols = ['SEX', 'PATIENT_TYPE', 'INTUBED', 'PNEUMONIA', 'ICU', 'CLASIFFICATION_FINAL']
    for col in int_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)

    # Log transformed data preview
    print("Transformed Data Preview:")
    print(df.head())

    kwargs['ti'].xcom_push(key='cleaned_data', value=df.to_json())

def load(**kwargs):
    cleaned_data = kwargs['ti'].xcom_pull(key='cleaned_data', task_ids='transform')
    if cleaned_data is None:
        raise ValueError("No data found from transform task")

    df = pd.read_json(cleaned_data)

    # Database connection configuration
    db_config = {
        'user': 'airflow_u',
        'password': 'KARU55bime22!!',
        'host': 'localhost',
        'port': '5432',
        'database': 'airflow_db'
    }
    connection_string = f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"

    # Create a SQLAlchemy engine
    engine = create_engine(connection_string)

    # Save the cleaned data to PostgreSQL
    try:
        with engine.connect() as connection:  # Use engine.connect() to establish the connection
            df.to_sql('covid_data', con=connection, index=False, if_exists='replace')
        print("Cleaned data successfully loaded into PostgreSQL")
    except Exception as e:
        raise ValueError(f"Error saving data to database: {e}")

# Define the default_args for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
}

# Define the DAG
with DAG(
    dag_id='covid_etl_pipeline',
    default_args=default_args,
    description='An ETL pipeline for processing COVID data',
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False
) as dag:

    extract_task = PythonOperator(
        task_id='extract',
        python_callable=extract,
        provide_context=True
    )

    transform_task = PythonOperator(
        task_id='transform',
        python_callable=transform,
        provide_context=True
    )

    load_task = PythonOperator(
        task_id='load',
        python_callable=load,
        provide_context=True
    )

    extract_task >> transform_task >> load_task
