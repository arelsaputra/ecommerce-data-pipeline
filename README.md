# E-commerce Data Pipeline

## Overview
This project implements a **data pipeline** for an e-commerce platform, designed to process and analyze sales, customer, and order data. It leverages **Python**, **Apache Airflow**, and standard data processing libraries to automate data ingestion, transformation, and reporting.

---

## Project Structure
ecommerce_data_pipeline/
│
├── dags/ # Airflow DAGs (workflow definitions)
├── airflow_env/ # Local Python virtual environment (ignored in Git)
├── datasets/ # Raw and processed datasets (ignored in Git)
│ ├── raw/ # Original datasets
│ └── processed/ # Cleaned and transformed datasets
├── reports/ # Generated analysis reports (ignored in Git)
├── .gitignore # Git ignore rules
├── requirements.txt # Python dependencies
└── README.md # Project documentation


---

## Features
- Automated **ETL (Extract, Transform, Load)** pipeline using Airflow.
- Data cleaning and preprocessing for sales and customer datasets.
- Generation of summary reports including:
  - Total sales per day
  - Total customers and orders
  - Product sales distribution
- Easy integration with **Metabase** for visual dashboards.

---

## Example DAG
```python
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

def extract_data():
    print("Extracting data...")

def transform_data():
    print("Transforming data...")

def load_data():
    print("Loading data...")

with DAG(
    'ecommerce_pipeline',
    start_date=datetime(2025, 1, 1),
    schedule_interval='@daily',
    catchup=False
) as dag:

    task1 = PythonOperator(task_id='extract', python_callable=extract_data)
    task2 = PythonOperator(task_id='transform', python_callable=transform_data)
    task3 = PythonOperator(task_id='load', python_callable=load_data)

    task1 >> task2 >> task3
```

Example Dataset

The repository does not include raw datasets for size reasons.
Structure expected in datasets/raw/:

File	Description
Online_Retail.csv	Sales transaction data
Customers.csv	Customer information
Orders.csv	Order details

Processed data will be stored in datasets/processed/ after the pipeline runs.

Reports & Visualizations :
 - Daily sales summaries in CSV or Excel.
 - Charts for product sales distribution and customer segmentation.
 - Optionally integrated with Metabase dashboards (config files ignored in Git).
