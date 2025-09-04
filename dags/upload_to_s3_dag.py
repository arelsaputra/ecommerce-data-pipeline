from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python import PythonOperator
from datetime import datetime


def upload_to_s3():
    hook = S3Hook(aws_conn_id="aws_default")  
    bucket_name = "ecommerce-arel-retail"   
    key = "datasets/retail.csv"              
    filename = "/opt/airflow/datasets/online_retail.csv"  

    hook.load_file(
        filename=filename,
        bucket_name=bucket_name,
        key=key,
        replace=True
    )
    print(f"✅ File {filename} berhasil di-upload ke s3://{bucket_name}/{key}")


default_args = {
    "owner": "arel",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
}

with DAG(
    dag_id="upload_to_s3_dag",
    default_args=default_args,
    description="Upload dataset ke S3 bucket",
    schedule_interval=None,  
    catchup=False,
    tags=["s3", "upload"],
) as dag:

    upload_task = PythonOperator(
        task_id="upload_file_to_s3",
        python_callable=upload_to_s3
    )
