from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from dag_setting import *
from datetime import datetime, timedelta

default_args = {
    "owner" : "toodullboi",
    "retries" : 5,
    "retry_delay" : timedelta(minutes=5)
}

with DAG(
    dag_id = "Entire_situation_daily_dag",
    default_args=default_args,
    description = "crawls situation&daily data",
) as dag:

    entire_batter_daily_task = BashOperator(
        task_id = "entire_batter_daily",
        bash_command="python /opt/airflow/dags/repo/src/entire/entire_batter_daily.py"
    )

    entire_batter_situation_task = BashOperator(
        task_id = "entire_batter_situation",
        bash_command="python /opt/airflow/dags/repo/src/entire/entire_batter_situation.py"
    )

    entire_pitcher_daily_task = BashOperator(
        task_id = "entire_pitcher_daily",
        bash_command="python /opt/airflow/dags/repo/src/entire/entire_pitcher_daily.py"
    )
    
    entire_pitcher_situation_task = BashOperator(
        task_id = "entire_pitcher_situation",
        bash_command="python /opt/airflow/dags/repo/src/entire/entire_pitcher_situation.py"
    )

    startTask = EmptyOperator(task_id="stark_task")


    startTask >> [entire_batter_situation_task, entire_pitcher_situation_task]
    entire_batter_situation_task >> entire_batter_daily_task
    entire_pitcher_situation_task >> entire_pitcher_daily_task


