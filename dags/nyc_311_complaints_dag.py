from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Default configuration for 311 Complaints tasks
default_args = {
    'owner': 'eliran',
    'depends_on_past': False,
    'start_date': datetime(2026, 4, 24),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Environment and path constants

PROJECT_ROOT = "/home/developer/projects/spark-course-python/spark-kafka-project/Nyc_Project/dags/nyc_311_complaints_dag.py"
PYTHON_PATH_CMD = f"-e PYTHONPATH={PROJECT_ROOT}"

# Spark package dependencies for Kafka, Elasticsearch and Postgres
KAFKA_PKG = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0"
ELASTIC_POSTGRES_PKGS = "org.elasticsearch:elasticsearch-spark-30_2.12:7.13.2,org.postgresql:postgresql:42.6.0"

with DAG(
    dag_id='nyc_311_complaints_pipeline',
    default_args=default_args,
    description='End-to-end Medallion pipeline for NYC 311 Complaints data',
    schedule_interval=None,
    catchup=False,
    tags=['NYC', '311', 'Complaints', 'Lakehouse']
) as dag:

    # 1. Ingestion: Fetch 311 data from API and produce to Kafka
    # Runs in Airflow container; requires 'requests' and 'kafka-python'
    run_producer = BashOperator(
        task_id='run_producer_311',
        bash_command='python3 -u /opt/airflow/src/ingestion/producer_311_complaints.py'
    )

    # 2. Bronze Layer: Consume from Kafka and save as raw Parquet in MinIO
    # Executed via docker exec in the dev_env container using the shared Kafka package
    run_bronze = BashOperator(
        task_id='run_bronze_311',
        bash_command=f'docker exec {PYTHON_PATH_CMD} dev_env spark-submit --packages {KAFKA_PKG} {PROJECT_ROOT}/src/pipelines/bronze/bronze_311_complaints.py'
    )

    # 3. Silver Layer: Data cleaning and schema standardization
    # Note: Using your specified filename 'silver_311_complaines.py'
    run_silver = BashOperator(
        task_id='run_silver_311',
        bash_command=f'docker exec {PYTHON_PATH_CMD} dev_env spark-submit {PROJECT_ROOT}/src/pipelines/silver/silver_311_complaines.py'
    )

    # 4. Gold Layer: Aggregations and final sync to Elasticsearch & Postgres
    # Loads required JDBC and ES connectors at runtime
    run_gold = BashOperator(
        task_id='run_gold_311',
        bash_command=f'docker exec {PYTHON_PATH_CMD} dev_env spark-submit --packages {ELASTIC_POSTGRES_PKGS} {PROJECT_ROOT}/src/pipelines/gold/gold_311_complaines.py'
    )

    # Execution dependency chain
    run_producer >> run_bronze >> run_silver >> run_gold