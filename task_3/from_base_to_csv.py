from airflow.hooks.base import BaseHook
from sqlalchemy import create_engine, Table, MetaData
from airflow.models import Variable
from contextlib import contextmanager

import csv

@contextmanager
def get_connection():
    # Подключение к базе данных
    connection = BaseHook.get_connection(conn_id='my_postgres_conn')
    
    database = connection.schema 
    user = connection.login 
    password = connection.password 
    host = connection.host
    port = connection.port
    
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')

    try:
        yield engine
    finally:
        engine.dispose()


def from_base_to_csv():

    item = Variable.get("path_to_csv", deserialize_json=True)
    
    with get_connection() as engine:
    
        # Выполнение запроса на выборку данных из таблицы
        result = engine.execute("SELECT * FROM dm.dm_f101_round_f")
        rows = result.fetchall()
        
        # Получение наименований колонок таблицы
        columns = result.keys()
        
        # Создание файла CSV и запись данных в него
        with open(item, 'w', newline='') as file:
            writer = csv.writer(file, delimiter=';')
            
            # Запись наименований колонок в файл
            writer.writerow(columns)
            
            # Запись данных из таблицы в файл
            writer.writerows(rows)
    
    print("Данные успешно выгружены в файл CSV.")