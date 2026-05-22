from airflow import DAG
from airfloe.oprators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'ojashvi',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)}

with DAG(
    'nyc_taxi_batch_pipeline',
    default_args=default_args,
    description = 'Downloads NYC Taxi data and run simple Pyspark processing',
    schedule_interval='@monthly',
    start_date = datetime(2026, 1, 1),
    catchup = False,

)as dag:

    #Task 1: Download the data 
    #we use jinja templating {{date_interval_start...}} to automatically get the current month in YYYY-MM format
    download_taxi_data = BashOperator(
        task_id='download_raw_data',
        bash_command = ('curl -o /opt/airflow/data/raw/yellow_tripdata_{{ data_interval_start.strftime("%Y-%m") }}.parquet '
            'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{{ data_interval_start.strftime("%Y-%m") }}.parquet'
            )
    )

    #Task 2: Process the data with PySpark
    process_taxi_data = BashOperator(
        task_id='process__with_pyspark',
        bash_command = 'python /opt/airflow/scripts/process_taxi_data.py {{ data_interval_start.strftime("%Y-%m") }}'
    )

    download_taxi_data >> process_taxi_data