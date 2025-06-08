import logging
import os
from datetime import datetime, timedelta
from pathlib import Path
import mysql.connector
from airflow import DAG
from airflow.operators.python import PythonOperator
from dotenv import load_dotenv
from sqlalchemy import create_engine
import pandas as pd

default_args = {
    'owner': 'aiflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 4, 25),
    'retries': 2,
    'retry_delay': timedelta(seconds=5),
    'max_active_runs': 1,
}

def create_mysql_config(ti):
    env_path = Path(__file__).resolve().parent.parent / ".env"
    load_dotenv(dotenv_path=env_path)
    # Параметры подключения к MySQL
    config = {
        'user': os.environ.get('MYSQL_USER'),
        'password': os.environ.get('MYSQL_PASSWORD'),
        'host': os.environ.get('MYSQL_HOST'),
        'database': os.environ.get('MYSQL_DBNAME')
    }

    logging.info(config)
    ti.xcom_push(key='config', value=config)


def extract_categories(ti):
    config = ti.xcom_pull(task_ids='create_mysql_config_task', key='config')
    connection = None
    try:
        connection = mysql.connector.connect(**config)
        df = pd.read_sql("SELECT * FROM transactions_categories", connection)
        # Сериализуем DataFrame для надежной передачи через XCom
        ti.xcom_push(key='categories', value=df.to_json(orient='records'))
        return df
    except Exception as e:
        logging.error(f"Ошибка извлечения категорий: {e}", exc_info=True)
        raise
    finally:
        if connection and connection.is_connected():
            connection.close()


def extract_clients(ti):
    config = ti.xcom_pull(task_ids='create_mysql_config_task', key='config')
    connection = None
    try:
        connection = mysql.connector.connect(**config)
        df = pd.read_sql("SELECT * FROM clients", connection)
        #df["registration_date"] = pd.to_datetime(df["registration_date"], unit="ms")
        ti.xcom_push(key='clients', value=df.to_json(orient='records'))
        return df
    except Exception as e:
        logging.error(f"Ошибка извлечения клиентов: {e}", exc_info=True)
        raise
    finally:
        if connection and connection.is_connected():
            connection.close()


def load(ti):
    engine = None
    try:
        # Десериализуем DataFrame из JSON
        categories_json = ti.xcom_pull(task_ids='extract_categories_task', key='categories')
        clients_json = ti.xcom_pull(task_ids='extract_clients_task', key='clients')

        categories = pd.read_json(categories_json, orient='records')
        clients = pd.read_json(clients_json, orient='records')
        env_path = Path(__file__).resolve().parent.parent / ".env"
        load_dotenv(dotenv_path=env_path)
        engine = create_engine(
            f"postgresql://{os.environ.get('USER')}:{os.environ.get('PASSWORD')}@"
            f"{os.environ.get('HOST')}:{os.environ.get('PORT')}/{os.environ.get('DB_NAME')}"
        )

        with engine.begin() as conn:
            categories.to_sql('categories', conn, if_exists='replace', index=False)
            clients.to_sql('clients', conn, if_exists='replace', index=False)

    except Exception as e:
        logging.error(f"Ошибка загрузки: {e}", exc_info=True)
        raise
    finally:
        if engine:
            engine.dispose()



with DAG(
    'etl_mysql_to_postgres',
    default_args=default_args,
    description='etl-процесс из mysql в postgres',
    schedule_interval=None,
    catchup=False,
    tags=['etl', 'postgres', 'mysql']
) as dag:
    create_mysql_config_task = PythonOperator(
        task_id='create_mysql_config_task',
        python_callable=create_mysql_config,
        provide_context=True,
        retries=default_args['retries'],
        execution_timeout=timedelta(minutes=1)
    )
    extract_clients_task = PythonOperator(
        task_id='extract_clients_task',
        python_callable=extract_clients,
        provide_context=True,
        retries=default_args['retries'],
        execution_timeout=timedelta(minutes=1)
    )
    extract_categories_task = PythonOperator(
        task_id='extract_categories_task',
        python_callable=extract_categories,
        provide_context=True,
        retries=default_args['retries'],
        execution_timeout=timedelta(minutes=1)
    )
    load_task = PythonOperator(
        task_id='load_task',
        python_callable=load,
        provide_context=True,
        retries=default_args['retries'],
        execution_timeout=timedelta(minutes=1)
    )

    create_mysql_config_task >> extract_clients_task >> extract_categories_task >> load_task


