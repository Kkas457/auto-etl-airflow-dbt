"""
<img src="/usr/local/airflow/include/img/clickhouse.png" alt="source" width="300"/>

🌟 Data Pipeline: MY_DAG_ID 🌟

Процесс обновляет таблицы:
TABLES_LIST

Конфигурация ETL процесса: [Overview](file:///usr/local/airflow/dbt/target/index.html#!/overview)
"""

from airflow import DAG, Dataset
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',  # value replaced from schema.yml
    'start_date': 'START_TIME',
    'retries': 1,
}

with DAG(
    dag_id='dbt_template_dag',  # value replaced from schema.yml
    default_args=default_args,
    schedule=None,  # value replaced dynamically
    catchup=False,
    doc_md=__doc__
) as dag:
    # Placeholder for dataset tasks