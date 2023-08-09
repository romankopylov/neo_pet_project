from get_database_connection import connect_to_database
from airflow.models import Variable

import psycopg2
import csv
import re

def validate_date(date):
    pattern = r'^\d{4}-\d{2}-\d{2}$'
    return re.match(pattern, date) is not None


def write_to_csv(result, onDate):

    path = Variable.get("path_to_csv", deserialize_json=True)

    output_file_path = f"{path}output_{onDate}.csv"
    
    with open(output_file_path, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['On date: ' + str(result[0][0])])
        writer.writerow(['min credit >> ' + str(result[0][1])])
        writer.writerow(['min debet >> ' + str(result[0][2])])
        writer.writerow(['max_credit >> ' + str(result[0][3])])
        writer.writerow(['max_debet >> ' + str(result[0][4])])


def get_posting_data(date):
    
    with connect_to_database() as conn:
        cur = conn.cursor()
    
        cur.execute("SELECT * FROM ds.get_posting_data(%s);", (date,))
        result = cur.fetchall()
    
        cur.close()
        conn.close()
    
    return result

def main_func():
    onDate = input("Enter the date (YYYY-MM-DD): ")
    
    if validate_date(onDate):
        result = get_posting_data(onDate)
        
        if len(result) > 0:
            write_to_csv(result, onDate)
            print("Done!")
        else:
            print(f'No data for {onDate}')
    else:
        print("Invalid date format. Please enter the date in the format YYYY-MM-DD.")