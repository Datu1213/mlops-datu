import os
import sys
import mlflow
import mlflow.sklearn
import mlflow.spark
import ray
import json
import time
import requests
import pandas as pd
import warnings
from ray import tune, train
from ray.air.integrations.mlflow import MLflowLoggerCallback
from ray.tune.schedulers import ASHAScheduler

from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.sql.functions import col

from sklearn.datasets import load_iris
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression as SklearnLogisticRegression
from sklearn.metrics import accuracy_score

# --- 1. 全局配置 ---

# MLflow / MinIO (从你的Notebook cell 1)
MLFLOW_TRACKING_URI = "http://mlflow-server:5000"
MLFLOW_S3_ENDPOINT_URL = "http://minio:9000"
AWS_ACCESS_KEY_ID = "minioadmin"
AWS_SECRET_ACCESS_KEY = "minioadmin"

# Ray 
RAY_ADDRESS = "ray://ray-head:10001"

# Spark
SPARK_MASTER_URL = "spark://spark-master:7077"
DBT_VIEW_NAME = "default.stg_user_events"

# Model Serving
SERVING_URL = "http://mlflow-model-server:5001/invocations"

# Ignore normal warnings of Ray Sklearn
warnings.filterwarnings("ignore", category=UserWarning, module="ray")
warnings.filterwarnings("ignore", category=FutureWarning, module="sklearn")


def setup_mlflow_environment():
    """配置MLflow客户端的环境变量。"""
    print("--- 1. 配置 MLflow 客户端环境 ---")
    os.environ["MLFLOW_TRACKING_URI"] = MLFLOW_TRACKING_URI
    os.environ["MLFLOW_S3_ENDPOINT_URL"] = MLFLOW_S3_ENDPOINT_URL
    os.environ["AWS_ACCESS_KEY_ID"] = AWS_ACCESS_KEY_ID
    os.environ["AWS_SECRET_ACCESS_KEY"] = AWS_SECRET_ACCESS_KEY
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    print(f"MLFLOW_TRACKING_URI set to: {MLFLOW_TRACKING_URI}")

def setup_spark_session() -> SparkSession:
    """
    创建并配置Spark Session。
    它会自动使用挂载在 /opt/spark/conf/ 目录下的 spark-defaults.conf
    (该文件已包含Delta, Kafka, Hive的包和配置)
    """
    print("--- 2. 创建 Spark Session ---")
    try:
        spark = SparkSession.builder \
            .appName("MLOps_Pipeline_Script") \
            .master(SPARK_MASTER_URL) \
            .config("spark.hadoop.fs.s3a.endpoint", "minio:9000") \
            .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY_ID) \
            .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY) \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .enableHiveSupport() \
            .getOrCreate()
        spark._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "http://minio:9000")
        spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", "minioadmin")
        spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "minioadmin")
        spark._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")

        print(f"Spark Session (Version {spark.version}) 成功连接到 {SPARK_MASTER_URL}。")
        return spark
    except Exception as e:
        print(f"严重错误：无法创建Spark Session: {e}")
        print("请确保Spark Master和Thrift Server正在运行，并且conf/spark-defaults.conf已正确挂载。")
        sys.exit(1)

def train_sklearn_model(experiment_name: str, model_params = {"C": 0.5, "solver": "liblinear"}) -> tuple[str, str]:
    """
    训练一个简单的Sklearn模型并将其记录到MLflow。
    (逻辑来自你的 mlflow.ipynb cell 2)
    """
    print("--- 3. 开始训练 (Sklearn) ---")
    mlflow.set_experiment(experiment_name)
    
    X, y = load_iris(return_X_y=True)
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    
    with mlflow.start_run() as run:
        run_id = run.info.run_id
        print(f"Starting Sklearn run: {run_id}")
        
        model = SklearnLogisticRegression(**model_params)
        model.fit(X_train, y_train)
        accuracy = accuracy_score(y_test, model.predict(X_test))
        
        mlflow.log_params(model_params)
        mlflow.log_metric("accuracy", accuracy)
        
        # 准备一个输入示例以自动推断签名
        input_example = pd.DataFrame(X_train[:5], columns=load_iris().feature_names)
        
        mlflow.sklearn.log_model(
            model, 
            "sklearn-model", # 产物路径
            input_example=input_example
        )
        print(f"Sklearn run {run_id} complete. Accuracy: {accuracy:.4f}")
        return run_id, "sklearn-model"

def train_spark_model(spark: SparkSession, experiment_name: str) -> tuple[str, str]:
    """
    训练一个分布式的Spark MLlib管道并记录到MLflow。
    (逻辑来自你的 mlflow.ipynb cell 5 & 6)
    """
    
    print("--- 4. 开始训练 (Spark MLlib) ---")
    mlflow.set_experiment(experiment_name)
    
    
    # a. 加载数据
    try:
        silver_df = spark.table(DBT_VIEW_NAME)
        silver_df.cache()
        print(f"Successfully loaded dbt view '{DBT_VIEW_NAME}'. Rows: {silver_df.count()}")
    except Exception as e:
        print(f"FAILED to load dbt view '{DBT_VIEW_NAME}'. Error: {e}")
        return None, None

    # b. 特征工程
    features_df = silver_df.fillna(0.0, subset=['purchase_value'])
    label_indexer = StringIndexer(inputCol="event_type", outputCol="label")
    features_df = label_indexer.fit(features_df).transform(features_df)
    
    categorical_cols = ["user_id", "page"]
    indexers = [StringIndexer(inputCol=col, outputCol=f"{col}_index", handleInvalid="keep") for col in categorical_cols]
    
    feature_cols = [f"{col}_index" for col in categorical_cols] + ["purchase_value"]
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
    
    # c. 定义模型和管道
    lr = LogisticRegression(featuresCol="features", labelCol="label")
    pipeline = Pipeline(stages=indexers + [assembler, lr])

    # d. 训练和跟踪
    with mlflow.start_run() as run:
        run_id = run.info.run_id
        print(f"Starting Spark MLlib run: {run_id}")
        
        print("Artifact URI:", mlflow.get_artifact_uri())
        
        mlflow.log_param("model_type", "LogisticRegression (Spark MLlib)")
        mlflow.log_param("feature_cols", ", ".join(feature_cols))
        
        spark_model = pipeline.fit(features_df)

        # Use transformed_df which contains colume like "*_index"
        transformed_df = spark_model.transform(features_df)

        input_example = transformed_df.select(feature_cols).limit(5).toPandas()
        # input_example = mlflow.models.convert_input_example_to_serving_input(input_example)
        print("Logging Spark ML Pipeline to MLflow...")
        mlflow.spark.log_model(
            spark_model, 
            "spark-pipeline-model"
        )
        
        # (评估模型...)
        # ...
        
        print(f"Spark MLlib run {run_id} complete.")
        
        return run_id, "spark-pipeline-model"

def run_hyperparameter_tuning(experiment_name: str) -> dict:
    """
    运行Ray Tune HPO作业并自动记录到MLflow。
    (逻辑来自你的 mlflow.ipynb cell 8)
    """
    print("--- 5. 开始超参数调优 (Ray Tune HPO) ---")
    RAY_ADDRESS = "ray://ray-head:10001"
    try:
        ray.init(address=RAY_ADDRESS)
        print(f"--- Ray Client: 成功连接到 Ray Head ({RAY_ADDRESS}) ---")
    except Exception as e:
        print(f"--- Ray Client: 连接失败: {e} ---")
        return None

    def train_objective(config: dict):
        """Ray Tune的目标函数。"""
        X, y = load_iris(return_X_y=True)
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2)
        params = {"C": config["C"], "solver": config["solver"]}
        model = SklearnLogisticRegression(**params)
        model.fit(X_train, y_train)
        accuracy = accuracy_score(y_test, model.predict(X_test))
        # 你的代码使用了 tune.report，这是旧版API。
        # 我们使用 train.report()，这是你安装的ray 2.49.2的现代API。
        tune.report({"accuracy": accuracy})

    # param_space
    search_space = {
        "C": tune.loguniform(1e-4, 1e-1),
        "solver": tune.choice(["liblinear", "saga", "lbfgs"])
    }

    # MLflowLoggerCallback
    mlflow_callback = MLflowLoggerCallback(
        tracking_uri=MLFLOW_TRACKING_URI,
        experiment_name=experiment_name,
        save_artifact=False
    )
    
    # RunConfig
    run_config = train.RunConfig(
        name="hpo_logistic_regression",
        callbacks=[mlflow_callback],
        storage_path="/tmp/ray_results"
    )
    
    tune_config = tune.TuneConfig(
        metric="accuracy",
        mode="max",
        num_samples=20, # 总共运行20次
        scheduler=ASHAScheduler() # ASHA高级调度器
    )

    print("--- 启动 Ray Tune 超参数调优 (共 20 次试验) ---")
    
    tuner = tune.Tuner(
        train_objective,
        param_space=search_space,
        tune_config=tune_config,
        run_config=run_config,
    )
    
    results = tuner.fit()
    
    print("--- 调优完成! ---")
    best_result = results.get_best_result(metric="accuracy", mode="max")
    
    best_params = best_result.config
    best_accuracy = best_result.metrics['accuracy']
    
    print(f"最佳试验的 Accuracy: {best_accuracy:.4f}")
    print(f"最佳试验的超参数 (Config): {best_params}")
    
    ray.shutdown()
    return best_params

def register_model(run_id: str, model_artifact_path: str, model_name: str, alias: str):
    """
    注册一个模型并为其设置别名。
    (逻辑来自你的 mlflow.ipynb cell 3)
    """
    print(f"--- 6. 注册模型 '{model_name}' (来自 Run ID: {run_id}) ---")
    model_uri = f"runs:/{run_id}/{model_artifact_path}"
    
    try:
        model_version = mlflow.register_model(
            model_uri=model_uri,
            name=model_name
        )
        print(f"模型成功注册为 '{model_name}' 版本 {model_version.version}")
        time.sleep(5) # 等待注册表生效
        
        # (使用你找到的正确API: set_registered_model_alias)
        client = mlflow.MlflowClient()
        client.set_registered_model_alias(
            name=model_name,
            version=model_version.version,
            alias=alias
        )
        print(f"模型版本 {model_version.version} 成功晋升到 '{alias}' 别名。")
        
    except Exception as e:
        print(f"模型注册或晋升失败: {e}")

def call_model_server(model_name: str):
    """
    调用已部署的模型服务API。
    (逻辑来自你的 mlflow.ipynb cell 9)
    """
    print(f"--- 7. 调用已部署的模型API ({model_name}) ---")
    
    # (这个测试数据是针对 'iris_logistic_regression' (sklearn) 模型的)
    test_data = {
        "dataframe_split": {
            "columns": ["sepal length (cm)", "sepal width (cm)", "petal length (cm)", "petal width (cm)"],
            "data": [
                [5.1, 3.5, 1.4, 0.2], # 预期: 0
                [6.2, 3.4, 5.4, 2.3]  # 预期: 2
            ]
        }
    }
    
    headers = {'Content-Type': 'application/json'}
    
    try:
        # (我们假设 'mlflow-model-server' 正在服务这个模型)
        response = requests.post(SERVING_URL, data=json.dumps(test_data), headers=headers)
        
        if response.status_code == 200:
            print("--- 模型服务API调用成功 ---")
            print(f"Status Code: {response.status_code}")
            print(f"Prediction Response: {response.json()}")
        else:
            print(f"--- 模型服务API调用失败 ---")
            print(f"Status Code: {response.status_code}")
            print(f"Response Body: {response.text}")
            
    except Exception as e:
        print(f"--- 模型服务API调用异常 ---")
        print(f"无法连接到 {SERVING_URL}: {e}")

def main():
    """按顺序执行整个MLOps管道"""
    
    # --- Part 1: 设置 ---
    setup_mlflow_environment()
    # spark = setup_spark_session()
    
    # --- Part 2: Sklearn 训练 & 注册 ---
    # sklearn_run_id, sklearn_model_path = train_sklearn_model(experiment_name="sklearn_experiment")
    # if sklearn_run_id:
    #     register_model(sklearn_run_id, sklearn_model_path, 
    #                    model_name="iris_logistic_regression", 
    #                    alias="Staging")


    # --- Part 4: HPO Fine tune ---
    best_params = run_hyperparameter_tuning(experiment_name="ray_tune_hpo_experiment")
    if best_params:
        train_sklearn_model(experiment_name="sklearn_experiment", model_params=best_params)
    # Retrain the model use baset params

    #--- Part 5: Test Model Serving ---
    # (假设 'mlflow-model-server' 正在服务 "iris_logistic_regression")
    call_model_server(model_name="iris_logistic_regression")

if __name__ == "__main__":
    main()