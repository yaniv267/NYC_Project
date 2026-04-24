from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'eliran',
    'start_date': datetime(2026, 4, 23),
}

with DAG(
    dag_id='hello_world_test',
    default_args=default_args,
    schedule_interval=None,
    catchup=False
) as dag:

    test_task = BashOperator(
        task_id='say_hello',
        bash_command='echo "Airflow sees my files!"'
    )