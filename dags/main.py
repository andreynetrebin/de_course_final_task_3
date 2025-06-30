from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import subprocess
import pendulum

# Функция для генерации данных
def generate_data():
    result = subprocess.run(["python", "dags/generate_data.py"], capture_output=True, text=True)
    print(result.stdout)
    if result.returncode != 0:
        raise Exception(f"Ошибка при генерации данных: {result.stderr}")

# Функция для проверки данных
def validate_data():
    result = subprocess.run(["python", "dags/validate_data.py"], capture_output=True, text=True)
    print(result.stdout)
    if result.returncode != 0:
        raise Exception(f"Ошибка при проверке данных: {result.stderr}")

# Функция для очистки данных
def clean_data():
    result = subprocess.run(["python", "dags/clean_data.py"], capture_output=True, text=True)
    print(result.stdout)
    if result.returncode != 0:
        raise Exception(f"Ошибка при очистке данных: {result.stderr}")

# Функция для загрузки очищенных данных в PostgreSQL
def load_data_to_postgresql():
    result = subprocess.run(["python", "dags/load_data_to_postgresql.py"], capture_output=True, text=True)
    print(result.stdout)
    if result.returncode != 0:
        raise Exception(f"Ошибка при загрузке данных в PostgreSQL: {result.stderr}")

# Функция для выполнения аналитических операций
def perform_analytics():
    result = subprocess.run(["python", "dags/perform_analytics.py"], capture_output=True, text=True)
    print(result.stdout)
    if result.returncode != 0:
        raise Exception(f"Ошибка при выполнении аналитических операций: {result.stderr}")

# Функция для переноса данных в ClickHouse
def transfer_to_clickhouse():
    result = subprocess.run(["spark-submit", "dags/transfer_to_clickhouse.py"], capture_output=True, text=True)
    print(result.stdout)
    if result.returncode != 0:
        raise Exception(f"Ошибка при переносе данных в ClickHouse: {result.stderr}")

# Устанавливаем часовой пояс для Москвы
local_tz = pendulum.timezone("Europe/Moscow")

with DAG(
        dag_id='main',
        start_date=datetime(2023, 10, 1, tzinfo=local_tz),
        schedule_interval='45 12 * * 2',  # Каждый вторник в 12:45
        catchup=False,
        description='DAG для генерации, проверки, очистки, загрузки данных о продажах, выполнения аналитики и переноса данных в ClickHouse',
        default_args={
            'execution_timeout': timedelta(minutes=60),
        },
        timezone=local_tz
) as dag:
    # Определяем задачи
    generate_data_task = PythonOperator(
        task_id='generate_data',
        python_callable=generate_data,
    )

    validate_data_task = PythonOperator(
        task_id='validate_data',
        python_callable=validate_data,
    )

    clean_data_task = PythonOperator(
        task_id='clean_data',
        python_callable=clean_data,
    )

    load_data_task = PythonOperator(
        task_id='load_data_to_postgresql',
        python_callable=load_data_to_postgresql,
    )

    analytics_task = PythonOperator(
        task_id='perform_analytics',
        python_callable=perform_analytics,
    )

    transfer_task = PythonOperator(
        task_id='transfer_to_clickhouse',
        python_callable=transfer_to_clickhouse,
    )

    # Устанавливаем порядок выполнения задач
    generate_data_task >> validate_data_task >> clean_data_task >> load_data_task >> analytics_task >> transfer_task
