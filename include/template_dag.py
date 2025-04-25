"""

<img src="/usr/local/airflow/include/img/clickhouse.png" alt="source" width="300"/>

🌟 Data Pipeline: MY_MODEL 🌟

Процесс обновляет таблицы: **SCHEMA.TABLE_NAME** 

**Тип обновление**: UPDATE_TYPE

Конфигурация ETL процесса: [MY_MODEL](file:///usr/local/airflow/dbt/target/index.html#!/overview)

"""

import pendulum 
from pendulum import datetime

from airflow import DAG
from airflow.operators.bash_operator import BashOperator

# We're hardcoding this value here for the purpose of the demo, but in a production environment this
# would probably come from a config file and/or environment variables!
DBT_PROJECT_DIR = "/usr/local/airflow/dbt"


with DAG(
    "MY_DAG_ID",
    start_date=pendulum.now(),
    description="MY_DESCRIPTION",
    schedule_interval="MY_INTERVAL",
    catchup=False,
    doc_md=__doc__,
    default_args={
        "env": {
            "DBT_USER": "{{ conn.postgres.login }}",
            "DBT_ENV_SECRET_PASSWORD": "{{ conn.postgres.password }}",
            "DBT_HOST": "{{ conn.postgres.host }}",
            "DBT_SCHEMA": "{{ conn.postgres.schema }}",
            "DBT_PORT": "{{ conn.postgres.port }}",
        }
    },
) as dag:
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"dbt run --select MY_MODEL --profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR}",
    )
