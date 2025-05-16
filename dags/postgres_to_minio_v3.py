from airflow import DAG
from datetime import datetime
from include.db_to_minio import DatabaseToMinioParquetOperator  

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
}

with DAG(
    "postgres_to_minio__v2",
    default_args=default_args,
    description="Load Postgres table to MinIO as Parquet",
    schedule_interval="@daily",
    start_date=datetime(2025, 4, 24),
    catchup=False,
) as dag:

    # Insert example
    insert_task = DatabaseToMinioParquetOperator(
        task_id="insert_task",
        postgres_conn_id="postgres",
        source_table="my_test_table",
        minio_endpoint_url="http://minio:9002",
        minio_access_key="minioadmin",
        minio_secret_key="minioadmin",
        minio_bucket="datalake",
        goo="my_goo_insert",
        iss="my_iss",
        update_type="insert",
        watermark_col="created_at",
        table_name="my_table",
        # partition_cols=["region"],
    )

    # Insert_Update example
    insert_update_full_task = DatabaseToMinioParquetOperator(
        task_id="insert_update_full_task",
        postgres_conn_id="postgres",
        source_table="my_test_table",
        minio_endpoint_url="http://minio:9002",
        minio_access_key="minioadmin",
        minio_secret_key="minioadmin",
        minio_bucket="datalake",
        goo="my_goo_insert_update",
        iss="my_iss",
        watermark_col="created_at",
        update_type="insert_update",
        full_refresh=True,
        table_name="my_table",
    )

    # Insert_Update_Delete example
    insert_update_delete_task = DatabaseToMinioParquetOperator(
        task_id="insert_update_delete_task",
        postgres_conn_id="postgres",
        source_table="my_test_table",
        minio_endpoint_url="http://minio:9002",
        minio_access_key="minioadmin",
        minio_secret_key="minioadmin",
        minio_bucket="datalake",
        goo="my_goo_insert_update_delete",
        iss="my_iss",
        update_type="insert_update_delete",
        table_name="my_table",
    )