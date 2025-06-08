from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd
import clickhouse_connect

CLICKHOUSE_PARAMS = {
    "host": "ch_server",
    "port": 8123,
    "username": "click",
    "password": "click",
    "database": "default"
}

default_args = {
    'start_date': datetime(2025, 5, 23),
    'catchup': False
}

def extract_data_from_postgres(**context):
    hook = PostgresHook(postgres_conn_id='postgres')
    conn = hook.get_conn()
    clients = pd.read_sql("SELECT * FROM clients", conn)
    transactions = pd.read_sql("SELECT * FROM transactions", conn)
    categories = pd.read_sql("SELECT * FROM categories", conn)
    context['ti'].xcom_push(key='clients', value=clients.to_json())
    context['ti'].xcom_push(key='transactions', value=transactions.to_json())
    context['ti'].xcom_push(key='categories', value=categories.to_json())

def transform_data(**context):
    clients = pd.read_json(context['ti'].xcom_pull(key='clients'))
    transactions = pd.read_json(context['ti'].xcom_pull(key='transactions'))
    categories = pd.read_json(context['ti'].xcom_pull(key='categories'))

    df = clients.merge(transactions, on='user_id').merge(categories, on='category_id')

    df_grouped = df.groupby(['user_id', 'tier']).agg(
        total_transactions=pd.NamedAgg(column='transaction_id', aggfunc='count'),
        total_income=pd.NamedAgg(column='amount', aggfunc=lambda x: x[df['type'] == 'income'].sum()),
        total_expenses=pd.NamedAgg(column='amount', aggfunc=lambda x: x[df['type'] == 'expense'].sum()),
        unique_categories_used=pd.NamedAgg(column='category_id', aggfunc='nunique'),
    ).reset_index()

    df_grouped['net_balance'] = df_grouped['total_income'] - df_grouped['total_expenses']
    df_grouped = df_grouped.rename(columns={"user_id": "client_id"})

    context['ti'].xcom_push(key='result_df', value=df_grouped.to_json())

def load_to_clickhouse(**context):
    df = pd.read_json(context['ti'].xcom_pull(key='result_df'))
    client = clickhouse_connect.get_client(**CLICKHOUSE_PARAMS)

    client.command("""
        CREATE TABLE IF NOT EXISTS client_financial_profiles (
            client_id UInt32,
            tier String,
            total_transactions UInt32,
            total_income Float64,
            total_expenses Float64,
            unique_categories_used UInt32,
            net_balance Float64
        ) ENGINE = MergeTree() ORDER BY client_id
    """)

    client.insert_df('client_financial_profiles', df)

with DAG("financial_profile_etl",
         default_args=default_args,
         schedule_interval='@daily',
         tags = ['clickhouse', 'etl']) as dag:

    extract = PythonOperator(
        task_id="extract_data_from_postgres",
        python_callable=extract_data_from_postgres
    )

    transform = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data
    )

    load = PythonOperator(
        task_id="load_to_clickhouse",
        python_callable=load_to_clickhouse
    )

    extract >> transform >> load
