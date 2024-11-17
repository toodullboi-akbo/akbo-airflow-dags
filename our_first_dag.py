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
    dag_id = "Current_Year_KBO_Crawler",
    default_args=default_args,
    description = "crawls a year data from kbo",
) as dag:
    batter_yearly_task = BashOperator(
        task_id = "batter_yearly",
        bash_command="python /opt/airflow/dags/repo/src/entire_batter_yearly.py"
    )

    batter_daily_task = BashOperator(
        task_id = "batter_daily",
        bash_command="python /opt/airflow/dags/repo/src/entire_batter_daily.py"
    )

    batter_sit_task = BashOperator(
        task_id = "batter_situation",
        bash_command="python /opt/airflow/dags/repo/src/entire_batter_situation.py"
    )

    pitcher_yearly_task = BashOperator(
        task_id = "pitcher_yearly",
        bash_command="python /opt/airflow/dags/repo/src/entire_pitcher_yearly.py"
    )

    pitcher_daily_task = BashOperator(
        task_id = "pitcher_daily",
        bash_command="python /opt/airflow/dags/repo/src/entire_pitcher_daily.py"
    )
    
    pitcher_sit_task = BashOperator(
        task_id = "pitcher_situation",
        bash_command="python /opt/airflow/dags/repo/src/entire_pitcher_situation.py"
    )

    fielder_yearly_task = BashOperator(
        task_id = "fielder_yearly",
        bash_command="python /opt/airflow/dags/repo/src/entire_fielder.py"
    )

    runner_yearly_task = BashOperator(
        task_id = "runner_yearly",
        bash_command="python /opt/airflow/dags/repo/src/entire_runner.py"
    )

    startTask = EmptyOperator(task_id="stark_task")


    startTask >> [batter_yearly_task, pitcher_yearly_task, fielder_yearly_task, runner_yearly_task]
    batter_yearly_task >> batter_sit_task >> batter_daily_task
    pitcher_yearly_task >> pitcher_sit_task >> pitcher_daily_task
    fielder_yearly_task
    runner_yearly_task


    # batter_yearly_task >> batter_daily_task >> batter_sit_task >> pitcher_yearly_task >> pitcher_daily_task >> pitcher_sit_task >> fielder_yearly_task >> runner_yearly_task
