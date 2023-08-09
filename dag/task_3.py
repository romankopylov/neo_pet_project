from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

# Импорт функции для выполнения скрипта
from from_csv_to_base import from_csv_to_base
from from_base_to_csv import from_base_to_csv

# Аргументы DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2022, 1, 1),
}

# Создание экземпляра DAG
dag = DAG('task_3', default_args=default_args, schedule_interval=None)

# Определяем оператор PythonOperator для выполнения скрипта load_data_to_postgres
from_csv_to_base_task = PythonOperator(
    task_id='from_csv_to_base',
    python_callable=from_csv_to_base,
    dag=dag
)

from_base_to_csv_task = PythonOperator(
    task_id='from_base_to_csv',
    python_callable=from_base_to_csv,
    dag=dag
)

from_base_to_csv_task >> from_csv_to_base_task