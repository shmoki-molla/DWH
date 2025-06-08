from datetime import datetime
import os
from pathlib import Path

import requests
import pandas as pd
from xml.etree import ElementTree
from airflow import DAG
from airflow.operators.python import PythonOperator
from dotenv import load_dotenv
from sqlalchemy import create_engine
import logging
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Параметры DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 4, 8),
}

# Функция для получения данных о курсах валют
def get_currency_rates_df():
    url = "https://www.cbr.ru/scripts/XML_daily.asp"
    response = requests.get(url)
    root = ElementTree.fromstring(response.content)

    rates_data = []
    for valute in root.findall('Valute'):
        rates_data.append({
            'currency_code': valute.find('CharCode').text,
            'currency_name': valute.find('Name').text,
            'nominal': int(valute.find('Nominal').text),
            'rate': float(valute.find('Value').text.replace(',', '.')),
            'date': datetime.strptime(root.attrib['Date'], '%d.%m.%Y').date()
        })

    df = pd.DataFrame(rates_data)
    return df

# Функция для загрузки данных в PostgreSQL
def load_to_postgres(ti):
    # Получаем данные
    currency_df = get_currency_rates_df()
    # Получаем информацию о подключении из переменных окружения Airflow
    hook = PostgresHook(postgres_conn_id="postgres")
    connection = hook.get_conn()
    logging.info(f'Попытка подключения...')
    engine = hook.get_sqlalchemy_engine()
    logging.info("Подключение успешно!")

    currency_df.to_sql(
        'currency_rates',
        engine,
        if_exists='replace',
        index=False
    )
    connection.close()

    # Для проверки, выводим первые несколько строк из таблицы
    df_check = pd.read_sql('SELECT * FROM currency_rates LIMIT 5', engine)
    ti.xcom_push(key='loaded_data', value=df_check.to_dict(orient='records'))
    print(df_check)

with DAG(
    dag_id='currency_rates_to_postgres',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description='Конвертирует транзакции в рубли с учетом курса валют',
    tags=['currency', 'postgres'],
) as dag:

    load_task = PythonOperator(
        task_id='load_currency_rates',
        python_callable=load_to_postgres,
    )


    load_task