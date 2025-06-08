import os

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 5, 21),
    'depends_on_past': False,
}

with DAG(
    dag_id='spark_etl_dag',
    schedule_interval='@daily',
    default_args=default_args,
) as dag:

    run_etl = SparkSubmitOperator(
        task_id='run_etl',
        application='/opt/airflow/scripts/etl_financial_profile.py',
        conn_id='spark_default',
        jars='/opt/bitnami/spark/custom_jars/postgresql-42.7.5.jar,/opt/bitnami/spark/custom_jars/clickhouse-jdbc-0.8.5-all.jar',
        application_args=[
            '--postgres-jar', '/opt/bitnami/spark/custom_jars/postgresql-42.7.5.jar',
            '--clickhouse-jar', '/opt/bitnami/spark/custom_jars/clickhouse-jdbc-0.8.5-all.jar'
        ],
        env_vars={
            'JAVA_HOME': '/opt/bitnami/java',
            'SPARK_HOME': '/home/airflow/.local',  # Указываем правильный путь
            'PATH': '/home/airflow/.local/bin:/opt/bitnami/java/bin:$PATH'
        },
        spark_binary='/home/airflow/.local/bin/spark-submit',
        verbose=True
    )