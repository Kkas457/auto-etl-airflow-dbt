"""
<img src="/usr/local/airflow/include/img/clickhouse.png" alt="source" width="300"/>

🌟 Data Pipeline: MY_MODEL 🌟

Процесс обновляет таблицы: **SCHEMA.TABLE_NAME** 

**Тип обновление**: UPDATE_TYPE

Конфигурация ETL процесса: [MY_MODEL](file:///usr/local/airflow/dbt/target/index.html#!/overview)
"""

from airflow import DAG, Dataset
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'team_b', # value replaced from schema.yml
    'start_date': datetime(2025, 4, 1),
    'retries': 1,
}

with DAG(
    dag_id='dbt_bussiness_flow', # value replaced from schema.yml
    default_args=default_args,
    schedule_interval=None, # value replaced from schema.yml
    catchup=False,
    doc_md=__doc__
) as dag:

    run_stg_customers = BashOperator(
        task_id='run_stg_customers',
        bash_command="dbt run --select stg_customers --profiles-dir /usr/local/airflow/dbt/ --project-dir /usr/local/airflow/dbt/",
        outlets=[Dataset('dbt://stg_customers')],
    )


    run_stg_orders = BashOperator(
        task_id='run_stg_orders',
        bash_command="dbt run --select stg_orders --profiles-dir /usr/local/airflow/dbt/ --project-dir /usr/local/airflow/dbt/",
        outlets=[Dataset('dbt://stg_orders')],
    )


    run_stg_orders >> run_stg_customers