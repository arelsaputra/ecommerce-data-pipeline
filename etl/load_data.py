import os
import platform
import psycopg2
import pandas as pd
from psycopg2.extras import execute_batch
from minio import Minio
from minio.error import S3Error

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "online-retail-db.c7q8a4iw0xn5.ap-southeast-1.rds.amazonaws.com")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "retail_db")
POSTGRES_USER = os.getenv("POSTGRES_USER", "airflow")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "password_kamu")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minio")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minio123")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "ecommerce-data")

if platform.system() == "Windows":
    csv_file = "tmp/orders.csv"
else:
    csv_file = "/tmp/orders.csv"


def get_postgres_conn():
    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        database=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )


def load_to_postgres(df: pd.DataFrame, table_name: str):
    conn = get_postgres_conn()
    cur = conn.cursor()
    try:
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id SERIAL PRIMARY KEY,
            "InvoiceNo" VARCHAR(50),
            "StockCode" VARCHAR(50),
            "Description" TEXT,
            "Quantity" INTEGER,
            "InvoiceDate" TIMESTAMP,
            "UnitPrice" NUMERIC,
            "CustomerID" VARCHAR(50),
            "Country" VARCHAR(100)
        )
        """
        cur.execute(create_table_query)

        rows = [
            (
                row["InvoiceNo"],
                row["StockCode"],
                row["Description"],
                row["Quantity"],
                row["InvoiceDate"],
                row["UnitPrice"],
                str(row["CustomerID"]) if pd.notna(row["CustomerID"]) else None,
                row["Country"],
            )
            for _, row in df.iterrows()
        ]

        insert_query = f"""
        INSERT INTO {table_name} 
        ("InvoiceNo", "StockCode", "Description", "Quantity", 
         "InvoiceDate", "UnitPrice", "CustomerID", "Country")
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """

        execute_batch(cur, insert_query, rows, page_size=5000)

        conn.commit()
        print(f"[Postgres] {len(rows)} rows inserted into '{table_name}' ✅")
    except Exception as e:
        print("[Postgres] Error:", e)
        conn.rollback()
    finally:
        cur.close()
        conn.close()


def upload_to_minio(file_path: str, object_name: str):
    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False,
    )

    try:
        if not client.bucket_exists(MINIO_BUCKET):
            client.make_bucket(MINIO_BUCKET)

        client.fput_object(MINIO_BUCKET, object_name, file_path)
        print(f"[MinIO] File '{object_name}' berhasil di-upload ✅")
    except S3Error as e:
        print("[MinIO] Error:", e)


def load_excel_to_dataframe(file_path: str) -> pd.DataFrame:
    try:
        df = pd.read_excel(file_path)
        print(f"[Excel] Berhasil load data dari: {file_path}")
        return df
    except Exception as e:
        print(f"[Excel] Error loading file: {e}")
        raise


def main():
    try:
        excel_path = "/opt/airflow/datasets/online_retail.xlsx"
        df = load_excel_to_dataframe(excel_path)

        print("=" * 50)
        print("DEBUG: DATA STRUCTURE")
        print("=" * 50)
        print("Columns:", df.columns.tolist())
        print("Number of rows:", len(df))
        print("First 5 rows:")
        print(df.head())
        print("=" * 50)

        df.to_csv(csv_file, index=False)
        print(f"[Local] Data berhasil disimpan ke: {csv_file}")

        load_to_postgres(df, "orders")

        upload_to_minio(csv_file, "orders.csv")

        print("[ETL] Process completed successfully ✅")
    except Exception as e:
        print(f"[ETL] Process failed ❌: {e}")
        raise


if __name__ == "__main__":
    main()
