from __future__ import annotations
import pendulum
from airflow.sdk import dag, task # 使用最新的导入路径

@dag(
    dag_id="example_dynamic_dag",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    schedule=None,
    tags=["example", "dynamic"],
)
def example_dynamic_dag():
    # 1. 这个任务返回一个列表，代表需要处理的对象
    @task
    def get_files_to_process() -> list[str]:
        return ["file_a.txt", "file_b.txt", "file_c.txt"]

    # 2. 这是一个处理单个对象的任务模板
    @task
    def process_file(file_name: str):
        print(f"Processing {file_name}...")

    # 3. .expand()方法会为上一步返回的列表中的每一项
    #    都创建一个并行的process_file任务实例。
    # 类似于map函数。
    process_file.expand(file_name=get_files_to_process())

example_dynamic_dag()