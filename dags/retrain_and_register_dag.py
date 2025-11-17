from airflow.sdk import dag, task
from datetime import datetime
import subprocess

script_path = "/home/jovyan/work/retrain_and_register.py"

@dag(dag_id="retrain_and_register_dag",
    start_date=datetime(2025, 11, 12),
    # Trigered by Grafana Webhook
    schedule=None,
    tags=["retrain", "register"],
    catchup=False,)
def retrain_and_register_dag():
    @task(task_id="retrain_and_register")
    def retrain_and_register():
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

    retrain_and_register()


# [END simplest_dag]

retrain_and_register_dag()