from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import csv
import random
import os

# Параметры DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 4, 9),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Список возможных валют
currencies = ['USD', 'EUR', 'RUB', 'GBP', 'JPY']
# Список возможных category_id
category_ids = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]


def generate_transactions(ti):
    num_transactions = 100
    shared_folder = "/opt/airflow/shared_data"
    output_file = os.path.join(shared_folder, f"transactions_{datetime.now().strftime('%Y%m%d')}.csv")


    with open(output_file, 'w', newline='') as csvfile:
        fieldnames = ['transaction_id', 'user_id', 'amount', 'currency', 'date', 'category_id']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        for i in range(num_transactions):
            transaction_id = 1001 + i
            user_id = random.randint(1, 100)
            amount = random.randint(100, 2000)
            currency = random.choice(currencies)
            date = datetime.now().strftime('%Y-%m-%d')
            category_id = random.choice(category_ids)

            writer.writerow({
                'transaction_id': transaction_id,
                'user_id': user_id,
                'amount': amount,
                'currency': currency,
                'date': date,
                'category_id': category_id
            })
    ti.xcom_push(key='output_file', value=output_file)
    print(f"Файл {output_file} успешно сгенерирован")


def process_transactions(ti):
    output_file = ti.xcom_pull(task_ids='generate_csv', key='output_file')

    if output_file:
        file_size = os.path.getsize(output_file)
        print(f"Файл {output_file} существует и имеет размер {file_size} байт.")
    else:
        print("Файл не найден!")

# Определение DAG
with DAG(
    'generate_daily_transactions',
    default_args=default_args,
    description='Генерирует CSV файл транзакций каждый день',
    schedule_interval=None,
    catchup=False,
    tags=['csv', 'generation'],
) as dag:
    # Задача 1: Генерация CSV файла
    generate_csv = PythonOperator(
        task_id='generate_csv',
        python_callable=generate_transactions,
        provide_context=True,
    )

    # Задача 2: Обработка сгенерированного файла (пример)
    process_csv = PythonOperator(
        task_id='process_csv',
        python_callable=process_transactions,
        provide_context=True,
    )

    # Определение порядка выполнения задач
    generate_csv >> process_csv