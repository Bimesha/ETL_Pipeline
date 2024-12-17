from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import requests

# Function to fetch weather data from OpenWeatherMap API
def extract(**kwargs):
    api_key = 'd6dbc559887d2c7a79a59caad00d9439'  # OpenWeatherMap API key
    city = 'London'  # Name of your choice
    api_url = f'https://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}&units=metric'

    try:
        # API Request
        response = requests.get(api_url)
        response.raise_for_status()
        weather_data = response.json()

        # Convert to DataFrame
        df = pd.json_normalize(weather_data)
        
        print("Extracted Weather Data:")
        print(df.head())

        # Push raw data to XCom
        kwargs['ti'].xcom_push(key='raw_data', value=df.to_json())
    except Exception as e:
        raise ValueError(f"Error fetching weather data: {e}")

# Function to clean and transform the weather data
def transform(**kwargs):
    raw_data = kwargs['ti'].xcom_pull(key='raw_data', task_ids='extract')
    if raw_data is None:
        raise ValueError("No data found from extract task")
    
    df = pd.read_json(raw_data)

    # Select and rename relevant columns
    cleaned_df = df[[
        'name',  # City Name
        'main.temp',  # Temperature
        'main.humidity',  # Humidity
        'weather[0].description',  # Weather Description
        'wind.speed',  # Wind Speed
        'dt'  # Date/Time
    ]]
    cleaned_df.columns = ['City', 'Temperature (C)', 'Humidity (%)', 'Weather Description', 'Wind Speed (m/s)', 'Timestamp']

    # Convert timestamp to datetime
    cleaned_df['Timestamp'] = pd.to_datetime(cleaned_df['Timestamp'], unit='s')

    print("Transformed Weather Data:")
    print(cleaned_df.head())

    # Push cleaned data to XCom
    kwargs['ti'].xcom_push(key='cleaned_data', value=cleaned_df.to_json())

# Function to save the cleaned data to a CSV file
def load(**kwargs):
    cleaned_data = kwargs['ti'].xcom_pull(key='cleaned_data', task_ids='transform')
    if cleaned_data is None:
        raise ValueError("No data found from transform task")

    df = pd.read_json(cleaned_data)

    # Save to CSV
    output_path = "/mnt/d/JN VS/ETL_PIPELINE/Weather_Data.csv"
    df.to_csv(output_path, index=False)
    print(f"Cleaned weather data saved to {output_path}")

# Define default_args for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
}

# Define the DAG
with DAG(
    dag_id='weather_etl_pipeline',
    default_args=default_args,
    description='An ETL pipeline for fetching and processing weather data',
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

    # Define task dependencies
    extract_task >> transform_task >> load_task
