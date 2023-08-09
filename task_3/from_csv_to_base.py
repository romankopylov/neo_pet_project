from airflow.hooks.base import BaseHook
from airflow.models import Variable
from sqlalchemy import create_engine, Table, MetaData, Column, Integer, String, Boolean, Date
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from contextlib import contextmanager
from datetime import datetime
from set_log_data import set_message_to_log

import csv
import decimal

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

def from_csv_to_base():

    item = Variable.get("path_to_csv", deserialize_json=True)

    rows_inserted = 0
    
    with get_connection() as engine:
    
        # Создаем сессию
        Session = sessionmaker(bind=engine)
        session = Session()
        
        value = [('Start loading table dm.dm_f101_round_f_v2',)]
        set_message_to_log('begin', value)
        
        # Определение метаданных таблицы
        metadata = MetaData()
        table = Table('dm_f101_round_f_v2', metadata, autoload=True, autoload_with=engine, schema='dm')
        
        datafile = []
        # Открытие файла CSV для чтения данных
        try:
            with open(item, 'r') as file:
                reader = csv.reader(file, delimiter=';')
                datafile = list(reader)
        except Exception as e:
            value = [('ERROR: dm.dm_f101_round_f_v2 - Cannot read file *.csv',)]
            set_message_to_log('error', value)
    
            error_message = f'{str(e)}'
            value = [(error_message,)]
            set_message_to_log('inform', value)
            raise
         
        # Чтение наименований колонок из первой строки файла
        columns = datafile[0]
        # Удаляем строку с колонками
        datafile.pop(0)
        
        # Начинаем сессию
        session.begin()
        
        # Вставка данных из файла в таблицу
        for row in datafile:
            # Проверка наличия строки с совпадающими значениями колонок начало_месяца, конец_месяца, номер_счета и актуальность = true
            select_query = table.select().where(
                    (table.c.from_date == row[0]) &
                    (table.c.to_date == row[1]) &
                    (table.c.chapter == row[2]) &
                    (table.c.ledger_account == row[3]) &
                    (table.c.characteristic == row[4]) &
                    (table.c.actuality == True)
                )
            result = session.execute(select_query).fetchone()
                    
            if result:
                # Проверка совпадения значения колонок балансов
                differ = False
                for i in range(5, 29):
                    if row[i] == "": row[i] = 0
                    num_csv = decimal.Decimal(row[i])
                    num_csv = format(num_csv, '.8f')
                    num_base = '{:.8f}'.format(result[i])
                    
                    if num_base != num_csv: 
                        differ = True 
                        break
                        
                if differ:
                    # Обновление прежней строки
                    update_query = table.update().where(
                            (table.c.from_date == row[0]) &
                            (table.c.to_date == row[1]) &
                            (table.c.chapter == row[2]) &
                            (table.c.ledger_account == row[3]) &
                            (table.c.characteristic == row[4]) &
                            (table.c.actuality == True)
                        ).values(actuality=False)
        
                    try:
                        session.execute(update_query)
                    
                    except SQLAlchemyError as e:
                        session.rollback()
                
                        error_message = f'UPDATE_ERROR: dm.dm_f101_round_f_v2 - {str(e.args[0])}'[:250]        
                        value = [(error_message,)]
                        set_message_to_log('error', value)
        
                        error_message = f'BAD ROW: <{row[0]}:{row[1]}:{row[2]}:{row[3]}:{row[4]}>'
                        value = [(error_message,)]
                        set_message_to_log('inform', value)
                        continue
                        
                else:
                    continue
                            
            # Вставка новой строки
            dict = {columns[i]: row[i] for i in range(len(columns))}
            dict['insert_date'] = datetime.now()
            dict['actuality'] = True
            
            insert_query = table.insert().values(dict)
                
            try:
                session.execute(insert_query)
                session.commit()
                rows_inserted += 1
            except Exception as e:
                session.rollback()
                    
                error_message = f'INSERT_ERROR: dm.dm_f101_round_f_v2 - {str(e.args[0])}'[:250] 
                value = [(error_message,)]
                set_message_to_log('error', value)
        
                error_message = f'BAD ROW: <{row[0]}:{row[1]}:{row[2]}:{row[3]}:{row[4]}>'
                value = [(error_message,)]
                set_message_to_log('inform', value)
                
        # Конец цикла for row in datafile         
            
        value = [(f'Finish loading table dm.dm_f101_round_f_v2 - Inserted: {rows_inserted}',)]
        set_message_to_log('finish', value)
        
        # Закрытие соединения с базой данных
        session.close()
    
    print("Данные успешно загружены в базу данных.")