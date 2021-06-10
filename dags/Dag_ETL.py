from datetime import timedelta
import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from extract_web import ETL

## Specific argument for dag
default_args = {
    'owner': 'Cartoon',
    'depends_on_past': False,
    'start_date': datetime(2021, 6, 10),
    'email': ['puvanut95@hotmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

## Create DAG Instance to run daily
dag = DAG(
    'ETL_dag',
    default_args=default_args,
    description='First ETL process!',
    schedule_interval= '0 0 * * *'
)

## Create Airflow instance to run ETL with dag argument
run_ETL = PythonOperator(
                task_id='IMDBExtract_etl',
                python_callable=ETL,
                dag=dag
                )

## Run ETL
run_ETL