import datetime

from get_database_connection import connect_to_database
from set_log_data import set_message_to_log
from airflow.models import Variable


def fill_acc_turnover():

    item = Variable.get("my_current_date", deserialize_json=True)
    
    i_OnDate = datetime.datetime.strptime(item, '%Y-%m-%d').date()
    
    with connect_to_database() as conn:
        
        cursor = conn.cursor()

        value = [('Start loading table ds.fill_account_turnover_f',)]
        set_message_to_log('begin', value)
        
        try:            
            cursor.execute("CALL ds.fill_account_turnover_f(%s);", (i_OnDate,))
            conn.commit()
        except (Exception, ps.DatabaseError) as error:
            conn.rollback()
        
            error_message = 'ERROR: ds.fill_account_turnover_f' + str(error.args[0])        
            value = [(error_message,)]
            set_message_to_log('error', value)
        else:
            value = [('Finish loading table ds.fill_account_turnover_f',)]
            set_message_to_log('finish', value)

        value = [('Start loading table dm.fill_f101_round_f',)]
        set_message_to_log('begin', value)
        
        try:
            cursor.execute("CALL dm.fill_f101_round_f(%s);", (i_OnDate,))
            conn.commit()
        except (Exception, ps.DatabaseError) as error:
            conn.rollback()

            error_message = 'ERROR: dm.fill_f101_round_f' + str(error.args[0])        
            value = [(error_message,)]
            set_message_to_log('error', value)
        else:
            value = [('Finish loading table dm.fill_f101_round_f',)]
            set_message_to_log('finish', value)
       
        cursor.close()