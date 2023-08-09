import psycopg2.extras as extras

from get_database_connection import connect_to_database


def set_message_to_log(status, value):

    match status:
        case 'begin':
            log_query = "INSERT INTO logs.log_data (date_and_time, status, description) VALUES (CURRENT_TIMESTAMP, 'BEGIN_LOADING', %s)"
        case 'error':
            log_query = "INSERT INTO logs.log_data (date_and_time, status, description) VALUES (CURRENT_TIMESTAMP, 'ERROR', %s)"
        case 'finish':
            log_query = "INSERT INTO logs.log_data (date_and_time, status, description) VALUES (CURRENT_TIMESTAMP, 'FINISH_LOADING', %s)"
        case _:
            return 0
            
    with connect_to_database() as conn:
        cursor = conn.cursor()
        
        try:
            extras.execute_values(cursor, log_query, value)    
            conn.commit()
        except:
            conn.rollback
            cursor.close()
            return 1
    
        cursor.close()