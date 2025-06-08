import logging
import os
from pathlib import Path
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from dotenv import load_dotenv
from airflow.providers.postgres.hooks.postgres import PostgresHook

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 4, 22),
    'retries': 2,
    'retry_delay':timedelta(minutes=1)
}



def read_csv(ti):
    try:
        file_path = f"/opt/airflow/shared_data/transactions_{datetime.now().strftime('%Y%m%d')}.csv"
        df = pd.read_csv(file_path)
        logging.info("Транзакции успешно считаны из csv")
        ti.xcom_push(key='csv', value=df)
    except Exception as e:
        logging.error(f"Ошибка чтения CSV: {e}")
        raise


def read_currency_rates(ti):
    try:
        hook = PostgresHook(postgres_conn_id="postgres")
        engine = hook.get_sqlalchemy_engine()
        query = "SELECT currency_code, rate FROM currency_rates"
        rates = pd.read_sql(query, engine)
        logging.info("Валюты успешно считаны")
        ti.xcom_push(key='rates', value=rates)
    except Exception as e:
        logging.error(f"Ошибка чтения курсов валют: {e}")
        raise


def convert_amounts(ti):
    df = ti.xcom_pull(task_ids='read_csv_task', key='csv')
    rates = ti.xcom_pull(task_ids='read_rates_task', key='rates')
    rate_dict = dict(zip(rates['currency_code'], rates['rate']))
    rate_dict['RUB'] = 1.0  # Добавляем курс рубля

    df['amount'] = df.apply(
        lambda row: row['amount'] * rate_dict.get(row['currency'], 1.0),
        axis=1
    )
    logging.info("Валюты успешно конвертированы")
    ti.xcom_push(key='converted_transactions', value=df)


def load_to_postgres(ti):
    try:
        df = ti.xcom_pull(task_ids='convert_task', key='converted_transactions')
        hook = PostgresHook(postgres_conn_id="postgres")
        engine = hook.get_sqlalchemy_engine()
        df.to_sql(
            'transactions',
            engine,
            if_exists='replace',
            index=False
        )
        logging.info(f"Успешно загружено {len(df)} транзакций")
    except Exception as e:
        logging.error(f"Ошибка загрузки в PostgreSQL: {e}")
        raise

with DAG(
    'etl-transactions',
    default_args=default_args,
    description="ЕTL-процесс с транзакциями и валютами",
    schedule_interval=None,
    catchup=False,
    tags=['postgres', 'csv', 'etl']
) as dag:
    read_csv_task = PythonOperator(
        task_id='read_csv_task',
        python_callable=read_csv,
        provide_context=True,
        retries=default_args['retries'],
        execution_timeout=timedelta(minutes=1)
    )
    read_rates_task = PythonOperator(
        task_id='read_rates_task',
        python_callable=read_currency_rates,
        provide_context=True,
        retries=default_args['retries'],
        execution_timeout=timedelta(minutes=1)
    )
    convert_task = PythonOperator(
        task_id='convert_task',
        python_callable=convert_amounts,
        provide_context=True,
        retries=default_args['retries'],
        execution_timeout=timedelta(minutes=1)
    )
    load_task = PythonOperator(
        task_id='load_task',
        python_callable=load_to_postgres,
        provide_context=True,
        retries=default_args['retries'],
        execution_timeout=timedelta(minutes=1)
    )

    read_csv_task >> read_rates_task >> convert_task >> load_task


