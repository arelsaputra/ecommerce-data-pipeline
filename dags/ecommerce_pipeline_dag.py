from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
import os

sys.path.insert(0, '/opt/airflow/etl')

def run_load_data():
    from load_data import main
    main()

def upload_to_minio(filename, key):
    from minio import Minio
    client = Minio(
        endpoint=os.getenv("MINIO_ENDPOINT", "minio:9000"),
        access_key=os.getenv("MINIO_ACCESS_KEY", "minio"),
        secret_key=os.getenv("MINIO_SECRET_KEY", "minio123"),
        secure=False
    )
    bucket = os.getenv("MINIO_BUCKET", "ecommerce-data")
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)
    client.fput_object(bucket, key, filename)
    print(f"[MinIO] Uploaded {filename} to {bucket}/{key}")

default_args = {
    "owner": "arel",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
}

with DAG(
    dag_id="ecommerce_pipeline",
    default_args=default_args,
    description="End-to-end pipeline: Load & Transform ecommerce data",
    schedule_interval="@daily",
    catchup=False,
    tags=["ecommerce", "postgres", "etl", "minio"],
) as dag:

    load_task = PythonOperator(
        task_id="load_data",
        python_callable=run_load_data
    )

    daily_sales_task = BashOperator(
        task_id="daily_sales",
        bash_command=(
            "PGPASSWORD={{ var.value.POSTGRES_PASSWORD }} "
            "psql -h {{ var.value.POSTGRES_HOST }} -U {{ var.value.POSTGRES_USER }} "
            "-d {{ var.value.POSTGRES_DB }} "
            "-f /opt/airflow/sql/daily_sales.sql"
        )
    )

    top_products_task = BashOperator(
        task_id="top_products",
        bash_command=(
            "PGPASSWORD={{ var.value.POSTGRES_PASSWORD }} "
            "psql -h {{ var.value.POSTGRES_HOST }} -U {{ var.value.POSTGRES_USER }} "
            "-d {{ var.value.POSTGRES_DB }} "
            "-f /opt/airflow/sql/top_products.sql"
        )
    )

    sales_country_task = BashOperator(
        task_id="sales_per_country",
        bash_command=(
            "PGPASSWORD={{ var.value.POSTGRES_PASSWORD }} "
            "psql -h {{ var.value.POSTGRES_HOST }} -U {{ var.value.POSTGRES_USER }} "
            "-d {{ var.value.POSTGRES_DB }} "
            "-f /opt/airflow/sql/sales_per_country.sql"
        )
    )

    upload_daily_sales = PythonOperator(
        task_id="upload_daily_sales",
        python_callable=upload_to_minio,
        op_kwargs={
            "filename": "/opt/airflow/reports/daily_sales.csv",
            "key": "reports/daily_sales.csv"
        }
    )

    upload_top_products = PythonOperator(
        task_id="upload_top_products",
        python_callable=upload_to_minio,
        op_kwargs={
            "filename": "/opt/airflow/reports/top_products.csv",
            "key": "reports/top_products.csv"
        }
    )

    upload_sales_country = PythonOperator(
        task_id="upload_sales_country",
        python_callable=upload_to_minio,
        op_kwargs={
            "filename": "/opt/airflow/reports/sales_per_country.csv",
            "key": "reports/sales_per_country.csv"
        }
    )

    load_task >> [daily_sales_task, top_products_task, sales_country_task]
    daily_sales_task >> upload_daily_sales
    top_products_task >> upload_top_products
    sales_country_task >> upload_sales_country
