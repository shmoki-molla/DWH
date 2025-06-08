from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 4, 25),
    'max_active_runs': 1,
}

with DAG(
    dag_id="master_pipeline",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    description="Мастер даг для ETL-процесса из source в PostgreSQL",
    tags=['master']
) as dag:
    # 1. Триггеры генераторов
    trigger_transactions = TriggerDagRunOperator(
        task_id="trigger_generate_transactions",
        trigger_dag_id="generate_daily_transactions",
        execution_date="{{ execution_date }}",
    )

    trigger_users = TriggerDagRunOperator(
        task_id="trigger_generate_users",
        trigger_dag_id="generate_daily_users",
        execution_date="{{ execution_date }}",
    )

    trigger_currency = TriggerDagRunOperator(
        task_id="trigger_currency_rates",
        trigger_dag_id="currency_rates_to_postgres",
        execution_date="{{ execution_date }}",
    )

    # 2. Датчики
    wait_transactions = ExternalTaskSensor(
        task_id="wait_generate_transactions",
        external_dag_id="generate_daily_transactions",
        mode="poke"
    )

    wait_users = ExternalTaskSensor(
        task_id="wait_generate_users",
        external_dag_id="generate_daily_users",
        mode="poke"
    )

    wait_currency = ExternalTaskSensor(
        task_id="wait_currency_rates",
        external_dag_id="currency_rates_to_postgres",
        mode="poke"
    )

    # 3. Синхронизация
    all_done = EmptyOperator(task_id="all_generators_completed")

    # 4. Трансформеры
    trigger_etl_transactions = TriggerDagRunOperator(
        task_id="trigger_etl_transactions",
        trigger_dag_id="etl-transactions",
        wait_for_completion=True
    )

    trigger_etl_users = TriggerDagRunOperator(
        task_id="trigger_etl_users",
        trigger_dag_id="etl_mysql_to_postgres",
        wait_for_completion=True
    )

    # Оркестрация: генерация → ожидание → синхронизация → трансформ
    trigger_transactions >> wait_transactions
    trigger_users >> wait_users
    trigger_currency >> wait_currency

    [wait_transactions, wait_users, wait_currency] >> all_done
    all_done >> [trigger_etl_transactions, trigger_etl_users]
