from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime


def test_s3_connection():
    hook = S3Hook(aws_conn_id="aws_default")
    s3_client = hook.get_conn()
    response = s3_client.list_buckets()
    buckets = [bucket["Name"] for bucket in response["Buckets"]]
    print("Buckets:", buckets)
    return buckets


with DAG(
    dag_id="test_s3_conn",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["example", "s3"],
) as dag:

    test_task = PythonOperator(
        task_id="test_s3",
        python_callable=test_s3_connection,
    )
