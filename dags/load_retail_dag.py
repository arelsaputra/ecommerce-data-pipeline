from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

sys.path.insert(0, '/opt/airflow/etl')

def run_load_data():
    from load_data import main
    main()

default_args = {
    "owner": "arel",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="load_online_retail",
    default_args=default_args,
    description="Load Online Retail data from Excel into Postgres",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["retail", "postgres"],
) as dag:

    load_data = PythonOperator(
        task_id="load_excel_to_postgres",
        python_callable=run_load_data
    )