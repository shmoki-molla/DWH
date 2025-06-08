import logging

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd
import clickhouse_connect

CLICKHOUSE_PARAMS = {
    "host": "ch_server",  # важно, чтобы airflow и clickhouse были в одной docker-сети
    "port": 8123,
    "username": "click",
    "password": "click",
    "database": "default"
}

default_args = {
    'start_date': datetime(2025, 5, 23),
    'catchup': False
}

def extract_data(**context):
    hook = PostgresHook(postgres_conn_id='postgres')
    conn = hook.get_conn()

    clients = pd.read_sql("SELECT * FROM clients", conn)
    transactions = pd.read_sql("SELECT * FROM transactions", conn)
    categories = pd.read_sql("SELECT * FROM categories", conn)

    context['ti'].xcom_push(key='clients', value=clients.to_json())
    context['ti'].xcom_push(key='transactions', value=transactions.to_json())
    context['ti'].xcom_push(key='categories', value=categories.to_json())


def transform_tier_category_analysis(**context):
    clients = pd.read_json(context['ti'].xcom_pull(key='clients'))
    transactions = pd.read_json(context['ti'].xcom_pull(key='transactions'))
    categories = pd.read_json(context['ti'].xcom_pull(key='categories'))

    # Сначала объединяем transactions с clients
    df = transactions.merge(clients, on='user_id')

    # Затем объединяем с categories
    df = df.merge(categories, on='category_id')

    # Проверяем итоговые столбцы
    logging.info(f"Final DataFrame columns: {df.columns.tolist()}")

    # Определяем правильное имя столбца для имени клиента
    name_column = 'name_y' if 'name_y' in df.columns else 'name'

    # Группируем данные
    grouped = (
        df.groupby(['tier', name_column, 'type'], as_index=False)
        .agg(
            transaction_count=('transaction_id', 'count'),
            total_amount=('amount', 'sum'),
            avg_amount=('amount', 'mean')
        )
        .sort_values(['tier', 'type'])
    )

    # Переименовываем столбцы для ClickHouse
    grouped = grouped.rename(columns={name_column: 'name'})

    context['ti'].xcom_push(key='tier_category_df', value=grouped.to_json())

def load_to_clickhouse(**context):
    df = pd.read_json(context['ti'].xcom_pull(key='tier_category_df'))

    df = df.rename(columns={
        'name_client': 'name'  # Приводим к ожидаемому имени в ClickHouse
    })

    client = clickhouse_connect.get_client(**CLICKHOUSE_PARAMS)

    client.command("""
        CREATE TABLE IF NOT EXISTS tier_category_analysis (
            tier String,
            name String,
            type String,
            transaction_count UInt32,
            total_amount Float64,
            avg_amount Float64
        ) ENGINE = MergeTree() ORDER BY name
    """)

    client.insert_df('tier_category_analysis', df)

with DAG("tier_category_analysis_etl",
         default_args=default_args,
         schedule_interval=None,
         tags = ['clickhouse', 'etl']) as dag:

    extract = PythonOperator(
        task_id="extract_data",
        python_callable=extract_data
    )

    transform = PythonOperator(
        task_id="transform_data",
        python_callable=transform_tier_category_analysis
    )

    load = PythonOperator(
        task_id="load_data_to_clickhouse",
        python_callable=load_to_clickhouse
    )

    extract >> transform >> load
