from airflow.decorators import dag
from airflow.datasets import Dataset
from airflow.operators.empty import EmptyOperator
from pendulum import datetime


@dag(
    start_date=datetime(2024, 8, 1),
    schedule=[Dataset("s3://my-bucket/my-key/")],
    catchup=False,
)
def my_consumer_dag():

    EmptyOperator(task_id="empty_task")


my_consumer_dag()
