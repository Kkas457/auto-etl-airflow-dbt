from airflow.decorators import dag, task
from airflow.datasets import Dataset
from pendulum import datetime

@dag(
    start_date=datetime(2025, 3, 17),
    schedule=None,
    catchup=False,
)
def my_producer_dag():

    @task(outlets=[Dataset("s3://my-bucket/my-key/")])
    def my_producer_task():
        pass

    my_producer_task()


my_producer_dag()