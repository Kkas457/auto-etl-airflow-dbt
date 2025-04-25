from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import s3fs
from sqlalchemy import create_engine
import logging

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def load_parquet_from_minio_to_postgres():
    # MinIO config
    endpoint_url = 'http://minio:9002'
    access_key = 'minioadmin'
    secret_key = 'minioadmin'

    fs = s3fs.S3FileSystem(
        client_kwargs={'endpoint_url': endpoint_url},
        key=access_key,
        secret=secret_key
    )

    # Example path to your parquet files (adjust as needed)
    prefix = 'pegas/12345_mgov/12345_mgov_address/current/'

    # Postgres config (Airflow connection or direct URL)
    pg_engine = create_engine('postgresql://postgres:postgres@postgres_v2:5432/postgres')
    target_table = 'first_data'

    # List and filter for Parquet files
    files = fs.ls(prefix)
    parquet_files = [f for f in files if f.endswith('.parquet')]

    if not parquet_files:
        logging.info("No parquet files found.")
        return

    for file in parquet_files:
        logging.info(f"Reading {file} from MinIO")
        with fs.open(file, 'rb') as f:
            df = pd.read_parquet(f, engine='pyarrow')
        
        logging.info(f"Inserting {len(df)} rows into {target_table}")
        df.to_sql(target_table, pg_engine, if_exists='append', index=False)

        logging.info(f"Loaded {file} to Postgres successfully.")

with DAG(
    dag_id='load_parquet_minio_to_postgres',
    default_args=default_args,
    description='Load Parquet files from MinIO to Postgres',
    schedule_interval=None,
    start_date=datetime(2025, 4, 17),
    catchup=False,
    tags=['minio', 'postgres', 'parquet'],
) as dag:

    load_task = PythonOperator(
        task_id='load_parquet_to_postgres',
        python_callable=load_parquet_from_minio_to_postgres
    )

    load_task
