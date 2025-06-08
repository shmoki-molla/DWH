from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 5, 23),
    'catchup': False,
    'depends_on_past': False
}

with DAG(
    dag_id='master_clickhouse_dag',
    schedule_interval='@daily',
    default_args=default_args,
    description="Мастер даг для построения витрин в ClickHouse",
    tags=['master', 'clickhouse']
) as dag:

    wait_etl_master = ExternalTaskSensor(
        task_id='wait_etl_master',
        external_dag_id='master_pipeline',
        mode='poke'
    )

    trigger_financial_profile = TriggerDagRunOperator(
        task_id='trigger_financial_profile',
        trigger_dag_id='financial_profile_etl',
        execution_date="{{ execution_date }}"
    )

    trigger_tier_category = TriggerDagRunOperator(
        task_id='trigger_tier_category',
        trigger_dag_id='tier_category_analysis_etl',
        execution_date="{{ execution_date }}"
    )

    trigger_monthly_trends = TriggerDagRunOperator(
        task_id='trigger_monthly_trends',
        trigger_dag_id='monthly_financial_trends_etl',
        execution_date="{{ execution_date }}"
    )

    wait_etl_master >> trigger_financial_profile >> trigger_tier_category >> trigger_monthly_trends
