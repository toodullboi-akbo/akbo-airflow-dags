from airflow import DAG
from airflow.operators.bash import BashOperator

from datetime import datetime, timedelta

default_args = {
    "owner" : "toodullboi",
}

with DAG(
    dag_id = "our_first_dag",
    default_args=default_args,
    description = "This is mine",
) as dag:
    task1 = BashOperator(
        task_id = "first_task",
        bash_command="python /opt/airflow/dags/repo/src/printSomethingTest.py"
    )

    task1
