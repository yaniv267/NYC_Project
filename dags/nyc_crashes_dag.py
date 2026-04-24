from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Default arguments for the DAG tasks
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
# These paths match your project structure on the dev_env container
PROJECT_ROOT = "/home/developer/projects/spark-course-python/nyc_final_project"
# PYTHONPATH must be injected for Spark to recognize the 'src' module
PYTHON_PATH_CMD = f"-e PYTHONPATH={PROJECT_ROOT}"

# External package dependencies for Spark
KAFKA_PKG = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0"
ELASTIC_POSTGRES_PKGS = "org.elasticsearch:elasticsearch-spark-30_2.12:7.13.2,org.postgresql:postgresql:42.6.0"

# DAG Definition
with DAG(
    dag_id='nyc_vehicle_crashes_pipeline',
    default_args=default_args,
    description='End-to-end Medallion architecture for NYC Vehicle Crashes data',
    schedule_interval=None, # Trigger manually or change to '@hourly'
    catchup=False,
    tags=['NYC', 'Crashes', 'Lakehouse']
) as dag:

    # 1. Ingestion: Fetch data from API and produce messages to Kafka
    # This runs within the Airflow image using dependencies from your Dockerfile
    run_producer = BashOperator(
        task_id='run_producer_crashes',
        bash_command='python3 -u /opt/airflow/src/ingestion/producer_crashes.py'
    )

    # 2. Bronze: Consume from Kafka and store as raw Parquet files in MinIO
    # Executed via docker exec in the dev_env container. Uses 'availableNow=True' logic.
    run_bronze = BashOperator(
        task_id='run_bronze_crashes',
        bash_command=f'docker exec {PYTHON_PATH_CMD} dev_env spark-submit --packages {KAFKA_PKG} {PROJECT_ROOT}/src/pipelines/bronze/bronze_crashes.py'
    )

    # 3. Silver: Perform data cleansing, transformations, and schema enrichment
    run_silver = BashOperator(
        task_id='run_silver_crashes',
        bash_command=f'docker exec {PYTHON_PATH_CMD} dev_env spark-submit {PROJECT_ROOT}/src/pipelines/silver/silver_crahes.py'
    )

    # 4. Gold: Business aggregation and synchronization to Elasticsearch & Postgres
    # Loads required JDBC and ES connectors at runtime
    run_gold = BashOperator(
        task_id='run_gold_crashes',
        bash_command=f'docker exec {PYTHON_PATH_CMD} dev_env spark-submit --packages {ELASTIC_POSTGRES_PKGS} {PROJECT_ROOT}/src/pipelines/gold/gold_crashes_pipeline.py'
    )

    # Define the execution flow (Dependency chain)
    run_producer >> run_bronze >> run_silver >> run_gold