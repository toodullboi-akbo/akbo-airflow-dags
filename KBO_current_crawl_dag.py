from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from dag_setting import *
from datetime import datetime, timedelta
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator



default_args = {
    "owner" : "toodullboi",
    "retries" : 5,
    "retry_delay" : timedelta(minutes=5),
    'start_date': datetime(2025, 2, 22, tzinfo=SEOUL_TZ),
}

with DAG(
    dag_id = "Current_Year_KBO_Crawler",
    default_args=default_args,
    description = "crawls current year data from kbo",
    schedule_interval="0 6 * * *"
) as dag:
    current_batter_yearly_task = BashOperator(
        task_id = "current_batter_yearly",
        bash_command="python /opt/airflow/dags/repo/src/current/current_batter_yearly.py"
    )

    current_batter_daily_task = BashOperator(
        task_id = "current_batter_daily",
        bash_command="python /opt/airflow/dags/repo/src/current/current_batter_daily.py"
    )

    current_batter_situation_task = BashOperator(
        task_id = "current_batter_situation",
        bash_command="python /opt/airflow/dags/repo/src/current/current_batter_situation.py"
    )

    current_pitcher_yearly_task = BashOperator(
        task_id = "current_pitcher_yearly",
        bash_command="python /opt/airflow/dags/repo/src/current/current_pitcher_yearly.py"
    )

    current_pitcher_daily_task = BashOperator(
        task_id = "current_pitcher_daily",
        bash_command="python /opt/airflow/dags/repo/src/current/current_pitcher_daily.py"
    )
    
    current_pitcher_situation_task = BashOperator(
        task_id = "current_pitcher_situation",
        bash_command="python /opt/airflow/dags/repo/src/current/current_pitcher_situation.py"
    )

    current_fielder_task = BashOperator(
        task_id = "current_fielder",
        bash_command="python /opt/airflow/dags/repo/src/current/current_fielder.py"
    )

    current_runner_task = BashOperator(
        task_id = "current_runner",
        bash_command="python /opt/airflow/dags/repo/src/current/current_runner.py"
    )

    current_team_batter_task = BashOperator(
        task_id = "current_team_batter",
        bash_command="python /opt/airflow/dags/repo/src/current_team/current_team_batter.py"
    )
    current_team_pitcher_task = BashOperator(
        task_id = "current_team_pitcher",
        bash_command="python /opt/airflow/dags/repo/src/current_team/current_team_pitcher.py"
    )
    current_team_fielder_task = BashOperator(
        task_id = "current_team_fielder",
        bash_command="python /opt/airflow/dags/repo/src/current_team/current_team_fielder.py"
    )
    current_team_runner_task = BashOperator(
        task_id = "current_team_runner",
        bash_command="python /opt/airflow/dags/repo/src/current_team/current_team_runner.py"
    )

    submit_spark_job_player_team_stats_task = SparkKubernetesOperator(
        task_id = "submit_spark_job_player_team_stats",
        namespace = "airflow",
        application_file = "/src/spark/spark-submit-player-team-stats.yaml",
        kubernetes_conn_id = "spark_operator_connection"
    )


    startTask = EmptyOperator(task_id="stark_task")


    startTask >> [current_batter_yearly_task, current_pitcher_yearly_task, current_fielder_task, current_runner_task]
    current_batter_yearly_task >> current_batter_situation_task >> current_batter_daily_task
    current_pitcher_yearly_task >> current_pitcher_situation_task >> current_pitcher_daily_task
    current_fielder_task >> current_team_pitcher_task >> current_team_fielder_task
    current_runner_task >> current_team_batter_task >> current_team_runner_task
    [current_batter_daily_task, current_pitcher_daily_task, current_team_fielder_task, current_team_runner_task] >> submit_spark_job_player_team_stats_task


