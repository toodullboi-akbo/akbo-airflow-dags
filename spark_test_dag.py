from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

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

    # Define the SparkSubmitOperator task
    submit_spark_job = SparkSubmitOperator(
        task_id='submit_spark_job',
        application='/opt/airflow/dags/repo/src/spark/scala-dustmq_2.13-0.1.0-SNAPSHOT.jar',  
        conn_id='spark_default',
        java_class='main', 
        # application_args=['arg1', 'arg2'],  
        conf={'spark.executor.memory': '2g', 'spark.executor.cores': '1'},
        name='spark_airflow_job',
        verbose=True,
        packages='org.apache.spark:spark-core:3.5.2, org.apache.spark:spark-sql:3.5.2,' +
                'org.apache.spark:spark-hive:3.5.2, com.azure:azure-storage-blob:12.25.0, com.azure:azure-identity:1.11.2,'+
                'com.microsoft.azure:azure-storage:8.6.6, org.apache.hadoop:hadoop-azure:3.3.6, org.apache.hadoop:hadoop-azure-datalake:3.3.6,'+
                'org.apache.hadoop:hadoop-common:3.3.6, org.apache.hadoop:hadoop-client:3.3.6, org.apache.hadoop:hadoop-client-api:3.3.6,'+
                'org.apache.hadoop:hadoop-client-runtime:3.3.6' 
    )



    submit_spark_job
