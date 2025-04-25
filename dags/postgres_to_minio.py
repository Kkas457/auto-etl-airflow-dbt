from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from include.postgres_to_minio import PostgresToMinioParquetOperator  # Import the operator
from airflow.providers.postgres.hooks.postgres import PostgresHook # explicitly import PostgresHook
import logging

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Python function to create dummy data in Postgres
def create_postgres_data(table_name, postgres_conn_id):
    """
    Creates a dummy table in Postgres with sample data.
    """
    pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    engine = pg_hook.get_sqlalchemy_engine()

    # Use a context manager to handle the connection
    with engine.connect() as connection:
        connection.execute(f"DROP TABLE IF EXISTS {table_name}")
        connection.execute(f"""
            CREATE TABLE {table_name} (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255),
                age INTEGER,
                city VARCHAR(255),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        connection.execute(f"""
            INSERT INTO {table_name} (name, age, city) VALUES
                ('Alice', 30, 'New York'),
                ('Bob', 25, 'Los Angeles'),
                ('Charlie', 35, 'Chicago'),
                ('David', 28, 'Houston'),
                ('Eve', 22, 'Phoenix');
        """)
    logging.info(f"Successfully created dummy data in table {table_name}")
    engine.dispose() # explicitly dispose the engine

with DAG(
    dag_id='postgres_to_minio_parquet_example',
    default_args=default_args,
    schedule_interval=None,  # Run manually
    catchup=False,
    tags=['postgres', 'minio', 'parquet'],
    ) as dag:

    # Task to create dummy data in Postgres
    create_data_task = PythonOperator(
        task_id='create_postgres_data',
        python_callable=create_postgres_data,
        op_kwargs={
            'table_name': 'my_test_table',
            'postgres_conn_id': 'postgres',
        },
    )

    # Task to load data from Postgres to MinIO as Parquet
    load_to_minio_task = PostgresToMinioParquetOperator(
        task_id='load_postgres_to_minio',
        postgres_conn_id='postgres',
        source_table='my_test_table',
        minio_endpoint_url='http://minio:9002',
        minio_access_key='minioadmin',
        minio_secret_key='minioadmin',
        minio_bucket='datalake',
        minio_prefix='MTSZN/WMARKET',
        filename='labor.parquet',
        partition_cols=['city']
    )

    load_to_minio_with_query_task = PostgresToMinioParquetOperator(
        task_id='load_postgres_to_minio_with_query',
        postgres_conn_id='postgres',
        source_table='my_test_table',
        query="SELECT id, name, age, city FROM my_test_table WHERE age > 25",
        minio_endpoint_url='http://minio:9002',
        minio_access_key='minioadmin',
        minio_secret_key='minioadmin',
        minio_bucket='datalake',
        minio_prefix='MTSZN/WMARKET/query',
        filename='labor_query.parquet'
    )

    create_data_task >> load_to_minio_task
    create_data_task >> load_to_minio_with_query_task
