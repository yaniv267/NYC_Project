# from airflow import DAG
# from airflow.operators.bash import BashOperator
# from datetime import datetime, timedelta

# # Basic DAG configurations
# default_args = {
#     'owner': 'eliran',
#     'depends_on_past': False,
#     'start_date': datetime(2026, 4, 24),
#     'retries': 1,
#     'retry_delay': timedelta(minutes=5),
# }

# with DAG(
#     dag_id='nyc_parking_violations_pipeline',
#     default_args=default_args,
#     description='Pipeline for Parking Violations - Medallion Architecture',
#     schedule_interval=None, # Manual execution for now
#     catchup=False,
#     tags=['NYC', 'Parking', 'Medallion']
# ) as dag:

#     # Step 1: Ingestion / Producer
#     # Fetching data from API and sending to Kafka
#     task_producer = BashOperator(
#         task_id='run_producer_traffic_violations',
#         bash_command='python3 -u /opt/airflow/src/ingestion/producer_traffic_violations.py'        
#     )

#     # Step 2: Bronze Layer
#     # Reading from Kafka and writing to MinIO in raw format
#     task_bronze = BashOperator(
#         task_id='run_bronze_traffic_violations',
#         #bash_command='python /opt/airflow/src/pipelines/bronze/bronze_traffic_violations.py'
#         #bash_command='docker exec dev_env spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0 /home/developer/projects/spark-course-python/nyc_final_project/src/pipelines/bronze/bronze_traffic_violations.py'
#         bash_command='docker exec -e PYTHONPATH=/home/developer/projects/spark-course-python/nyc_final_project dev_env spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0 /home/developer/projects/spark-course-python/nyc_final_project/src/pipelines/bronze/bronze_traffic_violations.py'
#     )

#     # Step 3: Silver Layer
#     # Cleaning and processing data using Spark
#     task_silver = BashOperator(
#         task_id='run_silver_traffic_violation',
#         bash_command='docker exec -e PYTHONPATH=/home/developer/projects/spark-course-python/nyc_final_project dev_env spark-submit /home/developer/projects/spark-course-python/nyc_final_project/src/pipelines/silver/silver_nyc_traffic_violation.py'
#     )

#     # Step 4: Gold Layer
#     # Creating final metrics for BI
#     task_gold = BashOperator(
#         task_id='run_gold_traffic_violation',
#         #bash_command='docker exec -e PYTHONPATH=/home/developer/projects/spark-course-python/nyc_final_project dev_env spark-submit /home/developer/projects/spark-course-python/nyc_final_project/src/pipelines/gold/gold_traffic_viloation.py'
#         bash_command='docker exec -e PYTHONPATH=/home/developer/projects/spark-course-python/nyc_final_project dev_env spark-submit --packages org.elasticsearch:elasticsearch-spark-30_2.12:7.13.2,org.postgresql:postgresql:42.6.0 /home/developer/projects/spark-course-python/nyc_final_project/src/pipelines/gold/gold_traffic_viloation.py'
#     )

#     # Define task dependencies (Execution order)
#     task_producer >> task_bronze >> task_silver >> task_gold

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# הגדרות בסיסיות
default_args = {
    'owner': 'eliran',
    'depends_on_past': False,
    'start_date': datetime(2026, 4, 24),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# -----------------------------
# Project paths
# -----------------------------
PROJECT_ROOT_PARENT = "/home/developer/projects/spark-course-python/spark-kafka-project"
PROJECT_ROOT = f"{PROJECT_ROOT_PARENT}/Nyc_Project"

# -----------------------------
# Spark packages
# -----------------------------
KAFKA_PKG = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0"
GOLD_PKGS = "org.elasticsearch:elasticsearch-spark-30_2.12:7.13.2,org.postgresql:postgresql:42.6.0"

with DAG(
    dag_id='nyc_parking_violations_pipeline',
    default_args=default_args,
    description='Pipeline for Parking Violations - Medallion Architecture',
    schedule_interval=None,
    catchup=False,
    tags=['NYC', 'Parking', 'Medallion']
) as dag:

    # -------------------------
    # 1. Producer (Kafka ingestion)
    # -------------------------
    task_producer = BashOperator(
        task_id='run_producer_traffic_violations',
        bash_command='python3 -u /opt/airflow/src/ingestion/producer_traffic_violations.py'        
    )

    # -------------------------
    # 2. Bronze (Raw ingestion from Kafka)
    # -------------------------
    task_bronze = BashOperator(
        task_id='run_bronze_traffic_violations',
        bash_command=f'''
docker exec dev_env bash -c "
set -e
export PYTHONPATH=$PYTHONPATH:{PROJECT_ROOT_PARENT}
spark-submit --packages {KAFKA_PKG} {PROJECT_ROOT}/src/pipelines/bronze/bronze_traffic_violations.py
"
'''
    )

    # -------------------------
    # 3. Silver (Cleaning + transformations)
    # -------------------------
    task_silver = BashOperator(
        task_id='run_silver_traffic_violation',
        bash_command=f'''
docker exec dev_env bash -c "
set -e
export PYTHONPATH=$PYTHONPATH:{PROJECT_ROOT_PARENT}
spark-submit {PROJECT_ROOT}/src/pipelines/silver/silver_nyc_traffic_violation.py
"
'''
    )

    # -------------------------
    # 4. Gold (Business layer + Elasticsearch + Postgres)
    # -------------------------
    task_gold = BashOperator(
        task_id='run_gold_traffic_violation',
        bash_command=f'''
docker exec dev_env bash -c "
set -e
export PYTHONPATH=$PYTHONPATH:{PROJECT_ROOT_PARENT}
spark-submit --packages {GOLD_PKGS} {PROJECT_ROOT}/src/pipelines/gold/gold_traffic_viloation.py
"
'''
    )

    # סדר הרצה
    task_producer >> task_bronze >> task_silver >> task_gold