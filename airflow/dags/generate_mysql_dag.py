from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException
import mysql.connector
from datetime import datetime, timedelta
import random
import os
from dotenv import load_dotenv
import logging
from airflow.models import Variable
from pathlib import Path
from airflow.providers.mysql.hooks.mysql import MySqlHook


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 4, 22),
    'email_on_failure': True,
    'email': ['osolodkov2325@gmail.com'],
    'email_on_retry': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
    'max_active_runs': 1,
}
def create_config(ti):
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


def generate_categories_table(ti):
    config = ti.xcom_pull(task_ids='create_config_task', key='config')
    connection = mysql.connector.connect(**config)
    cursor = connection.cursor(dictionary=True)
    try:
        # Создаем таблицу, если не существует
        cursor.execute("""
                CREATE TABLE IF NOT EXISTS transactions_categories (
                    category_id INT AUTO_INCREMENT PRIMARY KEY,
                    name VARCHAR(100) NOT NULL,
                    type ENUM('income', 'expense') NOT NULL
                )
            """)

        # Проверяем, пуста ли таблица
        cursor.execute("SELECT COUNT(*) as count FROM transactions_categories")
        result = cursor.fetchone()

        if result['count'] == 0:
            # Добавляем стандартные категории
            categories = [
                (1, 'Salary', 'income'),
                (2, 'Freelance', 'income'),
                (3, 'Investments', 'income'),
                (4, 'Gifts', 'income'),
                (5, 'Rent', 'expense'),
                (6, 'Food', 'expense'),
                (7, 'Transport', 'expense'),
                (8, 'Entertainment', 'expense'),
                (9, 'Healthcare', 'expense'),
                (10, 'Education', 'expense')
            ]

            cursor.executemany(
                "INSERT INTO transactions_categories (category_id, name, type) VALUES (%s, %s, %s)",
                categories
            )
            connection.commit()
            logging.info(f"Добавлено {len(categories)} категорий")
        else:
            logging.info("Таблица categories уже существует и содержит данные")

    except mysql.connector.Error as err:
        logging.error(f"Ошибка при работе с таблицей categories: {err}")
        raise
    finally:
        cursor.close()

def generate_clients(ti) -> None:
    config = ti.xcom_pull(task_ids='create_config_task', key='config')
    """Генерация клиентов в MySQL таблице."""
    logging.info('Подключаемся к базе))')
    logging.info(config)
    connection = None
    cursor = None
    print('Попытка подключения...')
    try:
        # Подключение к базе данных
        connection = mysql.connector.connect(**config)
        cursor = connection.cursor(dictionary=True)  # Используем dictionary cursor

        # Очистка таблицы перед вставкой новых данных
        cursor.execute("TRUNCATE TABLE clients")
        print("Таблица clients очищена")

        # Генерация 100 клиентов
        tiers = ['basic', 'premium']
        weights = [70, 30]
        names = ['Alex', 'Maria', 'John', 'Anna', 'Michael', 'Sophia', 'David', 'Emma', 'Daniel', 'Olivia']
        surnames = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Miller', 'Davis', 'Garcia', 'Rodriguez',
                    'Wilson']

        clients_data = []
        for i in range(1, 101):
            user_id = i
            name = f"{random.choice(names)} {random.choice(surnames)}"
            registration_date = datetime.now() - timedelta(days=random.randint(1, 365 * 3))
            tier = random.choices(tiers, weights=weights, k=1)[0]

            clients_data.append((user_id, name, registration_date, tier))

        # Пакетная вставка данных (более эффективная)
        cursor.executemany(
            "INSERT INTO clients (user_id, name, registration_date, tier) VALUES (%s, %s, %s, %s)",
            clients_data
        )

        connection.commit()
        print(f"Успешно добавлено {len(clients_data)} клиентов")

    except mysql.connector.Error as err:
        print(f"Ошибка MySQL: {err}")
        connection.rollback()  # Откатываем изменения при ошибке
        raise AirflowException(f"MySQL error: {err}")  # Пробрасываем исключение для Airflow
    except Exception as err:
        print(f"Неожиданная ошибка: {err}")
        raise AirflowException(f"Unexpected error: {err}")
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()
            print("Соединение с MySQL закрыто")


with DAG(
        'generate_daily_users',
        default_args=default_args,
        description='Генерирует таблицу пользователей каждый день',
        schedule_interval=None,
        catchup=False,
        tags=['mysql', 'generation'],
        doc_md="""### Ежедневная генерация клиентов\n\n
    Этот DAG ежедневно обновляет таблицу клиентов, генерируя 100 тестовых записей.\n
    Клиенты делятся на базовых (70%) и премиум (30%).""",  # Добавим документацию
) as dag:
    create_config_task = PythonOperator(
        task_id='create_config_task',
        python_callable=create_config,
        provide_context=True,
        retries=default_args['retries'],
        execution_timeout=timedelta(minutes=1)
    )
    generate_categories_task = PythonOperator(
        task_id='generate_categories_task',
        python_callable=generate_categories_table,
        provide_context=True,
        retries=default_args['retries'],
        execution_timeout=timedelta(minutes=1),
    )
    generate_task = PythonOperator(
        task_id='generate_clients',
        python_callable=generate_clients,
        provide_context=True,
        retries=default_args['retries'],
        execution_timeout=timedelta(minutes=1),
    )


    create_config_task >> generate_categories_task >> generate_task