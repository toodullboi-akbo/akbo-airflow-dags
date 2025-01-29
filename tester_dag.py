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
        task_id = "entire_pitcher_situation",
        bash_command="python /opt/airflow/dags/repo/src/entire/entire_pitcher_situation.py"
    )




    testTask
