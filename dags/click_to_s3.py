from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from clickhouse_driver import connect
import pandas as pd
import s3fs
import logging

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def export_clickhouse_to_minio():
    conn = connect(host='clickhouse', port='9000', password='admin', user='admin', connect_timeout=3600)
    logging.info('Connected successfully')

    dt = datetime(2025, 2, 14, 15, 30, 45)
    is_name = '12345_mgov'
    table_name = 'address'

    fs = s3fs.S3FileSystem(
        client_kwargs={"endpoint_url": 'http://minio:9002'},
        key='minioadmin',
        secret='minioadmin'
    )

    try:
        with conn.cursor() as cursor:
            cursor.execute('select * from default.first_data limit 2000000')
            logging.info('SQL query executed')

            i = 41
            file_path_act = f'pegas/{is_name}/{is_name}_{table_name}/current/cks.parquet'
            logging.info(f"Initial active file path: {file_path_act}")

            if fs.exists(file_path_act):
                fs.rm(f"pegas/{is_name}/{is_name}_{table_name}/current/*.parquet")
                logging.info("Old active parquet files removed")

            while True:
                data = cursor.fetchmany(5000000)

                if not data:
                    break

                columns = ['id', 'name', 'value', 'event_time']
                df = pd.DataFrame(data, columns=columns)

                file_path_hist = f'pegas/{is_name}/{is_name}_{table_name}/{dt.strftime("%Y")}/{dt.strftime("%m")}/{dt.strftime("%d")}/cks_{i}.parquet'
                file_path_act = f'pegas/{is_name}/{is_name}_{table_name}/current/cks_{i}.parquet'

                with fs.open(file_path_act, "wb") as f:
                    df.to_parquet(f, engine="pyarrow", index=False)

                with fs.open(file_path_hist, "wb") as f:
                    df.to_parquet(f, engine="pyarrow", index=False)

                logging.info(f"File saved to MinIO: {file_path_hist}, {file_path_act}")
                i += 1

    except Exception as ex:
        logging.error("Error while exporting ClickHouse data to MinIO", exc_info=True)
        raise ex
    finally:
        conn.close()
        logging.info("Connection closed")

with DAG(
    'export_clickhouse_to_minio_dag',
    default_args=default_args,
    description='Export data from ClickHouse to MinIO as parquet files',
    schedule_interval=None,
    start_date=datetime(2025, 4, 17),
    catchup=False,
    tags=['clickhouse', 'minio', 'parquet'],
) as dag:

    export_task = PythonOperator(
        task_id='export_clickhouse_to_minio',
        python_callable=export_clickhouse_to_minio
    )

    export_task
