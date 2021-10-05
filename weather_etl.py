from datetime import timedelta,datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

from weather_app import run_weather_collection

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2021, 10, 3),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'WEATHER_DAG',
    default_args=default_args,
    description='DAG to run the weather ETL process!',
    schedule_interval=timedelta(minutes=20),
)

run_etl = PythonOperator(
    task_id='Weather App ETL Run!',
    python_callable=run_weather_collection,
    dag=dag,
)

run_etl
