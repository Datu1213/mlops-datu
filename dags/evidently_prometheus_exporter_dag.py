# from __future__ import annotations
# import pendulum
# from airflow.sdk import dag, task
# from airflow.providers.docker.operators.docker import DockerOperator
# from docker.types import Mount

# MY_EXPORTER_IMAGE = "ghcr.io/datu1213/model-drift-exporter:1.1.1"
# MY_SHARED_NETWORK = "airflow-docker-project_default"

# @dag(
#   dag_id="evidently_prometheus_exporter_dag",
#     # timezone template: pendulum.timezone("Region/City")
#     start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
#     # 如果设置为True，并且start_date是一个过去的时间，
#     # Airflow会尝试为你“补上”从开始日期到现在所有错过的运行周期。
#     catchup=False,
#     # DAG的运行周期，这里设置为每天凌晨3点运行一次
#     # template: "min hour day month day_of_week"
#     # @daily, @hourly, @weekly等快捷方式也可以使用
#     schedule="1 * * * *",
#     tags=["promtheum_exporter"],
# )
# def evidently_prometheus_exporter_dag():
#     # @task.docker(
#     #     task_id="run_drift_check",

#     #     # 1. 镜像：使用你构建和推送的"瑞士军刀"镜像
#     #     image=MY_EXPORTER_IMAGE,
#     #     container_name="exporter",
#     #     # 2. 自动清理：运行成功后自动删除容器
#     #     auto_remove='never',

#     #     # 3. 退出日志：将容器的stdout/stderr打印到Airflow日志
#     #     tty=False,
#     #     mount_tmp_dir=False,
#     #     # 4. 网络：(最关键的一步)
#     #     # 告诉这个临时容器加入我们现有的共享网络
#     #     # Check it.
#     #     # https://docker-py.readthedocs.io/en/stable/api.html#docker.types.Mount
#     #     network_mode=MY_SHARED_NETWORK,
#     #     mounts=[ Mount(source="spark-data", 
#     #                   target="/opt/spark/data", 
#     #                   type="volume") ]
#     # )
#     # def run_drift_check_task():
#     #   import subprocess
#     #   pypath = "/opt/exporter-venv/bin/python"
#     #   filepath = "eviently-prometheus-exporter.py"
#     #   subprocess.run([pypath, filepath])
        
#     #   # 打印命令的标准输出
#     #   # print(")))))))))))))):", result.stdout)

#     # run_drift_check_task()
#     @task
#     def run_export(task_id="run_drift_check"):
#       import subprocess
#       dockerRun = "docker start"
#       container_name = "export"
#       subprocess.run([dockerRun, container_name])

# evidently_prometheus_exporter_dag()



from airflow.sdk import dag, task
from datetime import datetime
import subprocess

@dag(
    dag_id="evidently_prometheus_exporter_dag",
    start_date=datetime(2025, 11, 12),
    schedule="2 * * * *", # Run it per 2 minutes
    tags=["promtheum_exporter"],
    catchup=False,
)
def start_existing_container():
    @task(task_id="run_drift_check")
    def start_container():
        # Call docker CLI
        result = subprocess.run(
            ["docker", "run", "--rm", 
            "--name", "drift_check",
            "ghcr.io/datu1213/prometheus-exporter:1.1.4",
            "/opt/exporter-venv/bin/python", "eviently-prometheus-exporter.py"],
            capture_output=True,
            text=True
        )
        if result.returncode != 0:
            raise Exception(f"Start fail: {result.stderr}")
        print(result.stdout)

    start_container()

dag = start_existing_container()

