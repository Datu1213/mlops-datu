from airflow.sdk import dag, task
from datetime import datetime
import subprocess

@dag(dag_id="data_validation_exporter_dag",
    start_date=datetime(2025, 11, 12),
    # Trigered by Grafana Webhook
    schedule=None,
    tags=["validation", "data"],
    catchup=False,)
def data_validation_exporter_dag():
    @task(task_id="data_validation_exporter")
    def data_validation():
       # Call docker CLI
        result = subprocess.run(
            ["docker", "run", "--rm", 
            "--name", "data_validation",
            "ghcr.io/datu1213/prometheus-exporter:1.1.4",
            "/opt/exporter-venv/bin/python", "data_validation_exporter.py"],
            capture_output=True,
            text=True
        )
        if result.returncode != 0:
            raise Exception(f"Start fail: {result.stderr}")
        print(result.stdout)

    data_validation()

# [END simplest_dag]

data_validation_exporter_dag()