FROM apache/airflow:2.10.0-python3.12

USER root

COPY requirements.txt .
USER airflow
RUN pip install -r requirements.txt