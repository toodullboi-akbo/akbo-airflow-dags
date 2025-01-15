from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

from datetime import datetime, timedelta

default_args = {
    "owner" : "toodullboi",
}

with DAG(
    dag_id = "tester",
    default_args=default_args,
    description = "test whatever we want",
) as dag:
    testTask = BashOperator(
        task_id = "ls_test",
        bash_command="ls"
    )



    testTask
