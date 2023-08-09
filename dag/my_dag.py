from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

# Импорт функции для выполнения скрипта
from load_data_script import load_data_to_postgres
from fill_turnovers import fill_acc_turnover

# Аргументы DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2022, 1, 1),
}

# Создание экземпляра DAG
dag = DAG('my_dag', default_args=default_args, schedule_interval=None)

# Определяем оператор PythonOperator для выполнения скрипта load_data_to_postgres
load_data_task = PythonOperator(
    task_id='load_data_to_postgres',
    python_callable=load_data_to_postgres,
    dag=dag
)

fill_turnover_task = PythonOperator(
    task_id='fill_acc_turnover',
    python_callable=fill_acc_turnover,
    dag=dag
)

load_data_task >> fill_turnover_task
