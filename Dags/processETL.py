from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.baseoperator import chain
from datetime import datetime


default_args = {
    'owner': 'santiagoLinux',
    'depends_on_past': False,
    'start_date': datetime.now(),  
    'email': ['santiago.gomez_cas@uao.edu.co'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    'api__project_dag',
    default_args=default_args,
    description='Our first DAG with ETL process!',
    schedule_interval='@daily',
) as dag:
    pass


