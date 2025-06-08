from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import clickhouse_connect
import requests

# Конфигурация
CLICKHOUSE_HOST = 'clickhouse'  # имя сервиса в docker-compose
CLICKHOUSE_PORT = 8123
SUPERSET_URL = 'http://superset:8088'
SUPERSET_TOKEN = 'YOUR_SUPERSET_JWT_TOKEN'
DASHBOARD_ID = 5  # заменить на ваш ID

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

def fetch_clickhouse_data():
    client = clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT
    )
    result = client.query("""
        SELECT * FROM your_table_name
        WHERE year = 2025 AND month = 5
    """)
    print(result.result_rows)  # или сохраняйте/обрабатывайте как нужно

def refresh_superset_dashboard():
    headers = {
        'Authorization': f'Bearer {SUPERSET_TOKEN}',
        'Content-Type': 'application/json'
    }
    # Перезапуск дашборда через Superset Chart или Cache Warm-Up
    response = requests.post(
        f'{SUPERSET_URL}/api/v1/dashboard/{DASHBOARD_ID}/refresh',
        headers=headers
    )
    if response.status_code == 200:
        print("Dashboard refreshed successfully.")
    else:
        raise Exception(f"Failed to refresh dashboard: {response.text}")

with DAG(
    'generate_clickhouse_dashboard',
    default_args=default_args,
    start_date=datetime(2025, 5, 21),
    schedule_interval=None,
    catchup=False,
    tags=['clickhouse', 'superset']
) as dag:

    fetch_data = PythonOperator(
        task_id='fetch_clickhouse_data',
        python_callable=fetch_clickhouse_data
    )

    refresh_dashboard = PythonOperator(
        task_id='refresh_superset_dashboard',
        python_callable=refresh_superset_dashboard
    )

    fetch_data >> refresh_dashboard
