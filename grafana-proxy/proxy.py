from flask import Flask, request
import requests
from datetime import datetime, timezone

app = Flask(__name__)

def get_token():
    resp = requests.post(
        "http://airflow-apiserver:8080/auth/token",
        headers={"Content-Type": "application/json"},
        json={"username": "airflow", "password": "airflow"}
    )
    return resp.json()["access_token"]

@app.route("/retrain_and_register_dag", methods=["POST"])
def airflow_webhook():
    data = request.json
    token = get_token()
    resp = requests.post(
        "http://airflow-apiserver:8080/api/v2/dags/retrain_and_register_dag/dagRuns",
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}"
        },
        json={
          "dag_run_id": datetime.now().strftime("%Y%m%d%H%M%S%f"), # Unique ID
          "data_interval_start": datetime.utcnow().isoformat() + 'Z', # Use ISO 8601 format
          "data_interval_end": datetime.utcnow().isoformat() + 'Z',
          "logical_date": datetime.utcnow().isoformat() + 'Z',
          "run_after": datetime.utcnow().isoformat() + 'Z',
          "conf": {},
          "note": "Nothing"
        }
    )
    return resp.text, resp.status_code


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
