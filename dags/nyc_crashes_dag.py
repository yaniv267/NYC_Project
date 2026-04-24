from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# -----------------------------
# Default DAG configuration
# -----------------------------
default_args = {
    'owner': 'eliran',
    'depends_on_past': False,
    'start_date': datetime(2026, 4, 24),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# -----------------------------
# Project paths
# -----------------------------
PROJECT_ROOT_PARENT = "/home/developer/projects/spark-course-python/spark-kafka-project"
PROJECT_ROOT = f"{PROJECT_ROOT_PARENT}/Nyc_Project"

# PYTHONPATH for Spark jobs
PYTHON_PATH_CMD = f"export PYTHONPATH=$PYTHONPATH:{PROJECT_ROOT_PARENT}"

# -----------------------------
# Spark packages
# -----------------------------
KAFKA_PKG = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0"
ELASTIC_POSTGRES_PKGS = (
    "org.elasticsearch:elasticsearch-spark-30_2.12:7.13.2,"
    "org.postgresql:postgresql:42.6.0"
)

# -----------------------------
# DAG Definition
# -----------------------------
with DAG(
    dag_id='nyc_vehicle_crashes_pipeline',
    default_args=default_args,
    description='End-to-end Medallion architecture for NYC Vehicle Crashes data',
    schedule_interval='@hourly',
    catchup=False,
    tags=['NYC', 'Crashes', 'Lakehouse']
) as dag:

    # -------------------------
    # 1. Producer (Kafka ingestion)
    # -------------------------
    run_producer = BashOperator(
        task_id='run_producer_crashes',
        bash_command="""
python3 -u /opt/airflow/src/ingestion/producer_crashes.py
"""
    )

    # -------------------------
    # 2. Bronze (Raw ingestion from Kafka)
    # -------------------------
    run_bronze = BashOperator(
        task_id='run_bronze_crashes',
        bash_command=f'''
docker exec dev_env bash -c "
set -e
export PYTHONPATH=$PYTHONPATH:{PROJECT_ROOT_PARENT}
spark-submit --packages {KAFKA_PKG} {PROJECT_ROOT}/src/pipelines/bronze/bronze_crashes.py
"
'''
    )

    # -------------------------
    # 3. Silver (Cleaning + transformations)
    # -------------------------
    run_silver = BashOperator(
        task_id='run_silver_crashes',
        bash_command=f'''
docker exec dev_env bash -c "
set -e
export PYTHONPATH=$PYTHONPATH:{PROJECT_ROOT_PARENT}
spark-submit {PROJECT_ROOT}/src/pipelines/silver/silver_crashes.py
"
'''
    )

    # -------------------------
    # 4. Gold (Business layer + Elasticsearch + Postgres)
    # -------------------------
    run_gold = BashOperator(
        task_id='run_gold_crashes',
        bash_command=f'''
docker exec dev_env bash -c "
set -e
export PYTHONPATH=$PYTHONPATH:{PROJECT_ROOT_PARENT}
spark-submit --packages {ELASTIC_POSTGRES_PKGS} {PROJECT_ROOT}/src/pipelines/gold/gold_crashes_pipeline.py
"
'''
    )

    # -----------------------------
    # Task dependencies
    # -----------------------------
    run_producer >> run_bronze >> run_silver >> run_gold