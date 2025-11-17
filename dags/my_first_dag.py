from __future__ import annotations
import pendulum
from airflow.decorators import dag, task


@dag(
    dag_id="my_first_dag",
    # timezone template: pendulum.timezone("Region/City")
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    # 如果设置为True，并且start_date是一个过去的时间，
    # Airflow会尝试为你“补上”从开始日期到现在所有错过的运行周期。
    catchup=False,
    # DAG的运行周期，这里设置为每天凌晨3点运行一次
    # template: "min hour day month day_of_week"
    # @daily, @hourly, @weekly等快捷方式也可以使用
    schedule="0 3 * * *",
    tags=["my_test"],
)
def my_first_dag():
    @task
    def start_signal_task():
        return "Start Signal"

    @task
    def hello_task(start: str):
      return "Hello, Airflow!"

    @task
    def goodbye_task(start: str):
      return "Goodbye, Airflow!"

    @task
    def output_task(pre: str, post: str):
      print(f"{pre} and then {post}")

    start = start_signal_task()
    hello = hello_task(start)
    goodbye = goodbye_task(start)
    output_task(hello, goodbye)


my_first_dag()