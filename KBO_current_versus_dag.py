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
    'start_date': datetime(2025, 1, 1, tzinfo=SEOUL_TZ),
}

with DAG(
    dag_id = "Current_Year_KBO_versus_Crawler",
    default_args=default_args,
    description = "crawls current year versus data from kbo",
    schedule_interval="0 10 * * 1"
) as dag:
    current_versus_task = BashOperator(
        task_id = "current_batter_yearly",
        bash_command="python /opt/airflow/dags/repo/src/versus/versus_data.py"
    )

    submit_spark_job_versus_task = SparkKubernetesOperator(
        task_id = "submit_spark_job_player_team_stats",
        namespace = "airflow",
        application_file = "/src/spark/spark-submit-versus.yaml",
        kubernetes_conn_id = "spark_operator_connection"
    )


    startTask = EmptyOperator(task_id="stark_task")


    startTask >> current_versus_task >> submit_spark_job_versus_task