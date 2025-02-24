from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from datetime import datetime
import os

# Define the default arguments for the DAG
default_args = {
    'owner': 'toodullboi',
    'retries': 1
}

# Define the DAG
with DAG(
    dag_id='spark_submit_dag',
         default_args=default_args,
         catchup=False) as dag:

    submit_spark_job = SparkKubernetesOperator(
        task_id = "submit_spark_job",
        namespace = "airflow",
        application_file = "/opt/airflow/dags/repo/src/spark/spark-submit-player-team-stats.yaml",
        kubernetes_conn_id = "spark_operator_connection"
    )

    submit_spark_job
