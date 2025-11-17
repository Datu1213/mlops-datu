from __future__ import annotations
import pendulum
from airflow.sdk import dag, task
import subprocess  # 导入subprocess模块来执行shell命令

@dag(
    dag_id="pure_taskflow_data_passing_dag",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    schedule=None,
    tags=["example", "taskflow"],
)
def pure_taskflow_dag():
    @task
    def download_file() -> str:
        """
        创建一个临时文件并返回其路径。
        """
        import os
        file_path = "/tmp/my_data_pure.csv"
        with open(file_path, "w") as f:
            f.write("line 1\n")
            f.write("line 2\n")
            f.write("line 3\n")
        print(f"File created at: {file_path}")
        return file_path

    @task
    def count_lines(path_to_file: str):
        """
        这个Python函数接收一个文件路径字符串，并运行 wc -l 命令。
        """
        print(f"Task 'count_lines' received file path: {path_to_file}")
        # 使用subprocess模块来安全地执行shell命令
        result = subprocess.run(["wc", "-l", path_to_file], capture_output=True, text=True)
        
        # 打印命令的标准输出
        print("Output of 'wc -l':", result.stdout)
        
        # 任务的返回值是命令的输出结果
        return result.stdout.strip()

    file_path_value = download_file()
    count_result = count_lines(file_path_value)

pure_taskflow_dag()