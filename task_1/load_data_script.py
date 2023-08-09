import psycopg2 as ps
import psycopg2.extras as extras
import pandas as pd
import time

from get_database_connection import connect_to_database
from set_log_data import set_message_to_log
from airflow.models import Variable


def execute_values(conn, df, table, p_keys):

    # Создаем список значений для вставки
    insert_values = [tuple(row) for row in df.to_numpy()]

    # Создаем список столбцов для вставки
    columns = list(df.columns)
    insert_columns = ','.join(columns)

    # Создаем список столбцов-primary_key
    p_key_columns = ','.join(p_keys)
    
    # Создаем строку с обновлением значений
    update_values = ', '.join([f'{column} = excluded.{column}' for column in columns])
  
    # SQL запрос на добавление строк
    query = f"INSERT INTO {table} ({insert_columns}) VALUES %s ON CONFLICT ({p_key_columns}) DO UPDATE SET {update_values}"
    
    cursor = conn.cursor()

    # Добавление строки в таблицу LOG_DATA о начале загрузки
    value = [('Start loading table ' + table,)]
    set_message_to_log('begin', value)

    # Задержка 5 секунд
    time.sleep(5)
    
    try:
        extras.execute_values(cursor, query, insert_values)
        conn.commit()
    except (Exception, ps.DatabaseError) as error:
        # При ошибке отменяем изменения
        conn.rollback()

        error_message = 'ERROR: ' + table + ' ' + str(error.args[0])
        
        # Добавление строки в таблицу LOG_DATA об ошибке во время загрузки
        value = [(error_message,)]
        set_message_to_log('error', value)
    
        cursor.close()
        return 1
    
    # Добавление строки в таблицу LOG_DATA о завершении загрузки
    value = [('Finish loading table ' + table,)]
    set_message_to_log('finish', value)
    
    cursor.close()
# ---------------------Конец функции execute_values()----------------------------


def load_data_to_postgres():

    path = Variable.get("datafile_path", deserialize_json=True)
    
    with connect_to_database() as conn:
        # Загрузка данных из файла CSV в DataFrame
        df = pd.read_csv(path + 'ft_balance_f.csv', sep = ';')
        df.drop (df.columns [0], axis= 1 , inplace= True )
        
        # Запись данных в базу данных
        p_keys = ['ON_DATE', 'ACCOUNT_RK']
        execute_values(conn, df, 'DS.FT_BALANCE_F', p_keys)
        #---------------------------------------------------------------------------
           
        df = pd.read_csv(path + 'ft_posting_f.csv', sep = ';')
        df.drop (df.columns [0], axis= 1 , inplace= True )
        df = df.groupby(['OPER_DATE', 'CREDIT_ACCOUNT_RK', 'DEBET_ACCOUNT_RK'], as_index=False).sum()
        p_keys = ['OPER_DATE', 'CREDIT_ACCOUNT_RK', 'DEBET_ACCOUNT_RK']
        execute_values(conn, df, 'DS.FT_POSTING_F', p_keys)
        #---------------------------------------------------------------------------
          
        df = pd.read_csv(path + 'md_account_d.csv', sep = ';')
        df.drop (df.columns [0], axis= 1 , inplace= True )
        p_keys = ['DATA_ACTUAL_DATE', 'ACCOUNT_RK']
        execute_values(conn, df, 'DS.MD_ACCOUNT_D', p_keys)
        #---------------------------------------------------------------------------
            
        df = pd.read_csv(path + 'md_currency_d.csv', sep = ';')
        df.drop (df.columns [0], axis= 1 , inplace= True )
        p_keys = ['CURRENCY_RK', 'DATA_ACTUAL_DATE']
        execute_values(conn, df, 'DS.MD_CURRENCY_D', p_keys)
        #---------------------------------------------------------------------------
         
        df = pd.read_csv(path + 'md_exchange_rate_d.csv', sep = ';')
        df.drop(df.columns [0], axis= 1 , inplace= True)
        df = df.drop_duplicates()
        p_keys = ['DATA_ACTUAL_DATE', 'CURRENCY_RK']
        execute_values(conn, df, 'DS.MD_EXCHANGE_RATE_D', p_keys)
        #---------------------------------------------------------------------------
            
        df = pd.read_csv(path + 'md_ledger_account_s.csv', sep = ';')
        df.drop (df.columns [0], axis= 1 , inplace= True )
        df = df.fillna('')
         
        for i in range(len(df)):
            if not df.loc[i, 'PAIR_ACCOUNT'] == "":
                df.loc[i, 'PAIR_ACCOUNT'] = int(df.loc[i, 'PAIR_ACCOUNT'])
            
        p_keys = ['LEDGER_ACCOUNT', 'START_DATE']
        execute_values(conn, df, 'DS.MD_LEDGER_ACCOUNT_S', p_keys)
        #---------------------------------------------------------------------------
   