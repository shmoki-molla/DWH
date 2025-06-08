from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd
import clickhouse_connect

CLICKHOUSE_PARAMS = {
    "host": "ch_server",  # имя контейнера ClickHouse, убедись что оно корректное
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

def transform_monthly_financial_trends(**context):
    clients = pd.read_json(context['ti'].xcom_pull(key='clients'))
    transactions = pd.read_json(context['ti'].xcom_pull(key='transactions'))
    categories = pd.read_json(context['ti'].xcom_pull(key='categories'))

    # Merge all
    df = (
        transactions
        .merge(clients, on='user_id')
        .merge(categories, on='category_id')
    )

    df['date'] = pd.to_datetime(df['date'])
    df['year'] = df['date'].dt.year
    df['month'] = df['date'].dt.month

    agg = (
        df.groupby(['year', 'month', 'tier', 'type'], as_index=False)
        .agg(
            total_amount=('amount', 'sum'),
            active_clients=('user_id', pd.Series.nunique)
        )
        .sort_values(['year', 'month', 'tier'])
    )

    context['ti'].xcom_push(key='monthly_trends_df', value=agg.to_json())

def load_to_clickhouse(**context):
    df = pd.read_json(context['ti'].xcom_pull(key='monthly_trends_df'))

    client = clickhouse_connect.get_client(**CLICKHOUSE_PARAMS)

    client.command("""
        CREATE TABLE IF NOT EXISTS monthly_financial_trends (
            year UInt16,
            month UInt8,
            tier String,
            type String,
            total_amount Float64,
            active_clients UInt32
        ) ENGINE = MergeTree() ORDER BY (year, month, tier)
    """)

    client.insert_df('monthly_financial_trends', df)

with DAG("monthly_financial_trends_etl",
         default_args=default_args,
         schedule_interval=None,
         tags = ['clickhouse', 'etl']) as dag:

    extract = PythonOperator(
        task_id="extract_data",
        python_callable=extract_data
    )

    transform = PythonOperator(
        task_id="transform_data",
        python_callable=transform_monthly_financial_trends
    )

    load = PythonOperator(
        task_id="load_data_to_clickhouse",
        python_callable=load_to_clickhouse
    )

    extract >> transform >> load
