from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import os

def extract(**kwargs):
    file_path = '/mnt/d/JN VS/ETL_PIPELINE/covid_data.csv'
    try:
        # Load the CSV file and display preview
        df = pd.read_csv(file_path)
        print("Preview of Data:")
        print(df.head())
        kwargs['ti'].xcom_push(key='raw_data', value=df.to_json())
    except Exception as e:
        raise ValueError(f"Error loading file: {e}")

def transform(**kwargs):
    raw_data = kwargs['ti'].xcom_pull(key='raw_data', task_ids='extract')
    if raw_data is None:
        raise ValueError("No data found from extract task")

    df = pd.read_json(raw_data)

    # Cleaning and Transforming
    df.fillna(0, inplace=True)  # Fill missing values
    df['DATE_DIED'] = pd.to_datetime(df['DATE_DIED'], errors='coerce')
    df['SEX'] = df['SEX'].astype(int, errors='ignore')
    
    # Log transformed data preview
    print("Transformed Data:")
    print(df.head())

    kwargs['ti'].xcom_push(key='cleaned_data', value=df.to_json())

def load(**kwargs):
    cleaned_data = kwargs['ti'].xcom_pull(key='cleaned_data', task_ids='transform')
    if cleaned_data is None:
        raise ValueError("No data found from transform task")

    df = pd.read_json(cleaned_data)

    # Save the cleaned data to a new file
    output_path = "/mnt/d/JN VS/ETL_PIPELINE/Cleaned_Covid_Data.csv"
    df.to_csv(output_path, index=False)
    print(f"Cleaned data saved to {output_path}")

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