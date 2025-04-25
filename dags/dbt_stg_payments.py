"""
<img src="/usr/local/airflow/include/img/clickhouse.png" alt="source" width="300"/>

🌟 Data Pipeline: stg_payments 🌟

Процесс обновляет таблицы: **postgres.stg_payments** 

**Тип обновление**: INST

Конфигурация ETL процесса: [stg_payments](file:///usr/local/airflow/dbt/target/index.html#!/overview)
"""

from airflow import DAG, Dataset
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'team_c', # value replaced from schema.yml
    'start_date': datetime(2025, 4, 18),
    'retries': 1,
}

with DAG(
    dag_id='dbt_stg_payments', # value replaced from schema.yml
    default_args=default_args,
    schedule=[Dataset('dbt://stg_orders')], # value replaced from schema.yml
    catchup=False,
    doc_md=__doc__
) as dag:

    run_model = BashOperator(
        task_id='run_stg_payments',
        bash_command="dbt run --select stg_payments --profiles-dir /usr/local/airflow/dbt/ --project-dir /usr/local/airflow/dbt/",
        outlets=[Dataset('dbt://stg_payments')],
    )
