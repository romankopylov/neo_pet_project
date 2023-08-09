import psycopg2 as ps

from airflow.hooks.base import BaseHook
from contextlib import contextmanager

@contextmanager
def connect_to_database():

    # Получение информации о соединении
    connection = BaseHook.get_connection(conn_id='my_postgres_conn')

    conn = ps.connect(
        database = connection.schema, 
        user = connection.login, 
        password = connection.password, 
        host = connection.host, 
        port = connection.port)
    try:
        yield conn
    finally:
        conn.close()