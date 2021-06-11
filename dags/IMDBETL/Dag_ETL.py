from datetime import timedelta
import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from extract_web import extractWeb, transform_data, loadData

## Specific argument for dag
default_args = {
    'owner': 'Cartoon',
    'depends_on_past': False,
    'start_date': datetime.datetime(2021, 6, 10),
    'email': ['puvanut95@hotmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

## Create DAG Instance to run daily
dag = DAG(
    dag_id = 'ETL_dag',
    default_args=default_args,
    description='First ETL process!',
    schedule_interval= '@daily'
)

## Create Airflow instance to run ETL with dag argument
with dag:
    extract = PythonOperator(
                    task_id='extract',
                    python_callable=extractWeb
                    )
    
    transform = PythonOperator(
                    task_id='transform',
                    python_callable=transform_data
                    )
    
    load = PythonOperator(
                    task_id='loadtomongo',
                    python_callable=loadData
                    )
    
    extract >> transform >> load