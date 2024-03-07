import requests
from bs4 import BeautifulSoup
import numpy as np
import pandas as pd
import sqlite3
from datetime import datetime

url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
table_attribs = ['Name','MC_USD_Billion']
csv_path = 'Largest_banks_data.csv'
db_name = 'Banks.db'
table_name = 'Largest_banks'
csv_exchange_rate = pd.read_csv('exchange_rate.csv')
log_file = 'code_log.txt'


def log_progress(message):
    time_format = '%Y-%m-%d %H:%M:%S'
    time_now = datetime.now()
    time_stamp = time_now.strftime(time_format)
    with open(log_file,'a') as f:
        f.write(time_stamp + ' : ' + message + '\n')

log_progress('Preliminaries complete. Initiating ETL process')


def extract(target_url,table_attributes):
    url_text = requests.get(target_url).text
    html_parse = BeautifulSoup(url_text,'html.parser')
    html_tables = html_parse.find_all('tbody')
    html_rows = html_tables[0].find_all('tr')
    df = pd.DataFrame(columns=table_attributes)

    for row in html_rows:
        col = row.find_all('td')
        if len(col) != 0:
            data_dict = {'Name': col[1].find_all('a')[1].contents[0],
                         'MC_USD_Billion': float(col[2].contents[0].strip())}
            df1 = pd.DataFrame(data_dict,index=[0])
            df = pd.concat([df,df1],ignore_index=True)
    return df

extracted_data = extract(url,table_attribs)

log_progress('Data extraction complete. Initiating Transformation process')


def transform(extracted_data,csv_exchange_path):
    dict = csv_exchange_path.set_index('Currency').to_dict()['Rate']
    extracted_data['MC_GBP_Billion'] = [np.round(x*dict['GBP'],2) for x in extracted_data['MC_USD_Billion']]
    extracted_data['MC_EUR_Billion'] = [np.round(x*dict['EUR'],2) for x in extracted_data['MC_USD_Billion']]
    extracted_data['MC_INR_Billion'] = [np.round(x*dict['INR'],2) for x in extracted_data['MC_USD_Billion']]
    return extracted_data

transformed_data = transform(extracted_data,csv_exchange_rate)

log_progress('Data transformation complete. Initiating Loading process')


def load_to_csv(transformed_data):
    csv_file = transformed_data.to_csv(csv_path)
    return csv_file

load_to_csv(transformed_data)

log_progress('Data saved to CSV file')


sql_conn = sqlite3.connect(db_name)

log_progress('SQL Connection initiated')


def load_to_db(transformed_data,db_connection):
    db_file = transformed_data.to_sql(table_name,db_connection,if_exists='replace',index=False)
    return db_file

load_to_db(transformed_data,sql_conn)

log_progress('Data loaded to Database as a table, Executing queries')


def run_queries(query_statement,db_connection):
    print(query_statement)
    query_execution = pd.read_sql(query_statement,db_connection)
    print(query_execution)

query_statement1 = f'SELECT * FROM {table_name}'
run_queries(query_statement1,sql_conn)

query_statement2 = f'SELECT AVG(MC_GBP_Billion) FROM {table_name}'
run_queries(query_statement2,sql_conn)

query_statement3 = f'SELECT Name FROM {table_name} LIMIT 5'
run_queries(query_statement3,sql_conn)

log_progress('Process Complete')


sql_conn.close()

log_progress('Server Connection closed')
