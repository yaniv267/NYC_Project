from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Default configuration for Traffic Speed tasks
default_args = {
    'owner': 'eliran',
    'depends_on_past': False,
    'start_date': datetime(2026, 4, 24),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Project and execution constants
PROJECT_ROOT_PARENT = "/home/developer/projects/spark-course-python/spark-kafka-project"
PROJECT_ROOT = f"{PROJECT_ROOT_PARENT}/Nyc_Project"

# Spark package dependencies
KAFKA_PKG = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0"
ELASTIC_POSTGRES_PKGS = "org.elasticsearch:elasticsearch-spark-30_2.12:7.13.2,org.postgresql:postgresql:42.6.0"

with DAG(
    dag_id='nyc_traffic_speed_pipeline',
    default_args=default_args,
    description='End-to-end Medallion pipeline for NYC Traffic Speed data',
    schedule_interval=None,
    catchup=False,
    tags=['NYC', 'Traffic', 'Speed', 'Lakehouse']
) as dag:

    # 1. Ingestion: Fetch speed data and produce to Kafka
    run_producer = BashOperator(
        task_id='run_producer_traffic',
        bash_command='python3 -u /opt/airflow/src/ingestion/producer_traffic.py'
    )

    # 2. Bronze: Consume from Kafka and save as raw Parquet in MinIO
    run_bronze = BashOperator(
        task_id='run_bronze_traffic',
        bash_command=f'''
docker exec dev_env bash -c "
set -e
export PYTHONPATH=$PYTHONPATH:{PROJECT_ROOT_PARENT}
spark-submit --packages {KAFKA_PKG} {PROJECT_ROOT}/src/pipelines/bronze/bronze_traffic.py
"
'''
    )

    # 3. Silver: Data cleaning and speed metrics processing
    run_silver = BashOperator(
        task_id='run_silver_traffic',
        bash_command=f'''
docker exec dev_env bash -c "
set -e
export PYTHONPATH=$PYTHONPATH:{PROJECT_ROOT_PARENT}
spark-submit {PROJECT_ROOT}/src/pipelines/silver/silver_traffic.py
"
'''
    )

    # 4. Gold: Business aggregations and final sync to Elasticsearch & Postgres
    run_gold = BashOperator(
        task_id='run_gold_traffic',
        bash_command=f'''
docker exec dev_env bash -c "
set -e
export PYTHONPATH=$PYTHONPATH:{PROJECT_ROOT_PARENT}
spark-submit --packages {ELASTIC_POSTGRES_PKGS} {PROJECT_ROOT}/src/pipelines/gold/gold_traffic.py
"
'''
    )

  
    run_producer >> run_bronze >> run_silver >> run_gold