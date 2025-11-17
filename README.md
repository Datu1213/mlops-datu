# mlops-datu
MLOps structure from Data pipeline to ML pipeline.

# Structure

```txt
mlops-datu/
├── LICENSE
├── README.md
├── config
│   ├── airflow.cfg
│   └── spark-defaults.conf
├── config copy
│   ├── airflow.cfg
│   └── spark-defaults.conf
├── dags
│   ├── __pycache__
│   │   ├── evidently_prometheus_exporter_dag.cpython-312.pyc
│   │   ├── exmaple_dynamic_dag.cpython-312.pyc
│   │   ├── my_first_dag.cpython-312.pyc
│   │   ├── pure_taskflow_dag.cpython-312.pyc
│   │   ├── retrain_and_register_dag.cpython-312.pyc
│   │   └── task_dag.cpython-312.pyc
│   ├── data_validation_dag.py
│   ├── evidently_prometheus_exporter_dag.py
│   ├── exmaple_dynamic_dag.py
│   ├── my_first_dag.py
│   ├── pure_taskflow_dag.py
│   ├── retrain_and_register_dag.py
│   └── task_dag.py
├── docker-compose.yaml
├── exporter
│   ├── data_validation_exporter.py
│   ├── eviently-prometheus-exporter.py
│   ├── kafka_to_delta.py
│   ├── retrain_and_register.py
│   └── spark
│       └── data
├── grafana-proxy
│   └── proxy.py
├── hive-conf
│   └── hive-site.xml
├── infrastructure
│   └── infrastructure
│       └── terraform
│           ├── main.tf
│           ├── outputs.tf
│           ├── terraform.tfstate
│           ├── terraform.tfstate.backup
│           ├── tfplan
│           └── variables.tf
├── postgres-db-volume
│   └── hive-schema-3.1.0.postgres.sql
├── prometheus
│   └── conf
│       ├── alertmanager.yml
│       ├── alerts.yml
│       └── prometheus.yml
├── spark-notebooks
│   ├── data_drift_report.html
│   ├── data_drift_report_CORRUPTED.html
│   ├── evidently_prometheus.ipynb
│   ├── kafka_to_delta.ipynb
│   ├── mlflow.ipynb
│   └── retrain_and_register.py
├── kubernetes
├── infrastructure
├── kubernetes
└── src
    ├── consumer
    │   └── consumer.py
    ├── kafka_ml_processor.py
    └── producer
        └── producer.py
```