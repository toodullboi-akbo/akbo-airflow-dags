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
    dag_id = "Entire_and_Legacy_KBO_Crawler",
    default_args=default_args,
    description = "crawls (almost) whole data from kbo",
) as dag:
    entire_batter_yearly_task = BashOperator(
        task_id = "entire_batter_yearly",
        bash_command="python /opt/airflow/dags/repo/src/entire/entire_batter_yearly.py"
    )

    entire_batter_daily_task = BashOperator(
        task_id = "entire_batter_daily",
        bash_command="python /opt/airflow/dags/repo/src/entire/entire_batter_daily.py"
    )

    entire_batter_situation_task = BashOperator(
        task_id = "entire_batter_situation",
        bash_command="python /opt/airflow/dags/repo/src/entire/entire_batter_situation.py"
    )

    entire_pitcher_yearly_task = BashOperator(
        task_id = "entire_pitcher_yearly",
        bash_command="python /opt/airflow/dags/repo/src/entire/entire_pitcher_yearly.py"
    )

    entire_pitcher_daily_task = BashOperator(
        task_id = "entire_pitcher_daily",
        bash_command="python /opt/airflow/dags/repo/src/entire/entire_pitcher_daily.py"
    )
    
    entire_pitcher_situation_task = BashOperator(
        task_id = "entire_pitcher_situation",
        bash_command="python /opt/airflow/dags/repo/src/entire/entire_pitcher_situation.py"
    )

    entire_fielder_task = BashOperator(
        task_id = "entire_fielder",
        bash_command="python /opt/airflow/dags/repo/src/entire/entire_fielder.py"
    )

    entire_runner_task = BashOperator(
        task_id = "entire_runner",
        bash_command="python /opt/airflow/dags/repo/src/entire/entire_runner.py"
    )

    legacy_batter_task = BashOperator(
        task_id = "legacy_batter",
        bash_command="python /opt/airflow/dags/repo/src/entire/entire_legacy_batter.py"
    )

    legacy_pitcher_task = BashOperator(
        task_id = "legacy_pitcher",
        bash_command="python /opt/airflow/dags/repo/src/entire/entire_legacy_pitcher.py"
    )

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


    startTask >> [entire_batter_yearly_task, entire_pitcher_yearly_task, entire_fielder_task, entire_runner_task]
    entire_batter_yearly_task >> entire_batter_situation_task >> entire_batter_daily_task
    entire_pitcher_yearly_task >> entire_pitcher_situation_task >> entire_pitcher_daily_task
    entire_fielder_task >> legacy_batter_task >> team_batter_task >> team_runner_task
    entire_runner_task >> legacy_pitcher_task >> team_pitcher_task >> team_fielder_task


