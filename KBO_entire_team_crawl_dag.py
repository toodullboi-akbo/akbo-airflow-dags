from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

from datetime import datetime, timedelta

default_args = {
    "owner" : "toodullboi",
    "retries" : 5,
    "retry_delay" : timedelta(minutes=5)
}

with DAG(
    dag_id = "Entire_team_dag",
    default_args=default_args,
    description = "crawls team data",
) as dag:

    team_batter_task = BashOperator(
        task_id = "team_batter",
        bash_command="python /opt/airflow/dags/repo/src/entire_team/entire_team_batter.py"
    )
    team_pitcher_task = BashOperator(
        task_id = "team_pitcher",
        bash_command="python /opt/airflow/dags/repo/src/entire_team/entire_team_pitcher.py"
    )
    team_fielder_task = BashOperator(
        task_id = "team_fielder",
        bash_command="python /opt/airflow/dags/repo/src/entire_team/entire_team_fielder.py"
    )
    team_runner_task = BashOperator(
        task_id = "team_runner",
        bash_command="python /opt/airflow/dags/repo/src/entire_team/entire_team_runner.py"
    )


    startTask = EmptyOperator(task_id="stark_task")


    startTask >> [team_batter_task, team_pitcher_task, team_fielder_task, team_runner_task]


