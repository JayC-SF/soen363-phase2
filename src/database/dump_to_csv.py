import pandas as pd
import mysql.connector
from pathlib import Path
import os

from tqdm import tqdm
from utility.variables import DATABASE_HOST, DATABASE_NAME, DATABASE_PASSWORD, DATABASE_USER, DB_CSV_PATH

def convert_tables_to_csv():
    print("Get connection...")
    conn = mysql.connector.connect(
        host=DATABASE_HOST,
        user=DATABASE_USER,
        password=DATABASE_PASSWORD,
        database=DATABASE_NAME
    )
    cursor = conn.cursor()
    print("Getting all tables...")
    cursor.execute('SHOW TABLES')
    tables = cursor.fetchall()
    tables = [table[0] for table in tables]
    print("Creating csv folder path...")
    Path(DB_CSV_PATH).mkdir(parents=True, exist_ok=True)
    for table in tables:
        convert_table_to_df(cursor, conn.database, table, os.path.join(DB_CSV_PATH, f"{table}.csv"))

def convert_table_to_df(cursor, database_name, table_name, output_csv): 
    print(f"Fetching columns from {table_name}...")
    # query the columns
    query = f"SELECT column_name FROM information_schema.columns WHERE table_schema = '{database_name}' AND table_name = '{table_name}'"
    cursor.execute(query)
    rows = cursor.fetchall()
    # get the columns
    columns =  [row[0] for row in rows]
    # create a dictionary for the csv

    print(f"Fetching total number of records from {table_name}...")
    # query the number of records
    query = f"SELECT COUNT(*) FROM `{table_name}`"
    cursor.execute(query)
    (records_count,) = cursor.fetchone()
    print(f"Total number of records to parse: {records_count}")
    
    # create query to fetch all columns
    print(f"Fetching records from {table_name}, delete csv file if application is stopped ...")
    print(f"Fetching records from {table_name}...")
    data_query = f"SELECT { ','.join([f'`{col}`' for col in columns]) } FROM `{table_name}`"
    cursor.execute(data_query)
    batch_count = 50000
    include_header = True
    mode = 'w'
    with tqdm(total=records_count) as pbar:
        # get records
        while True:
            data_records = cursor.fetchmany(batch_count)
            if (len(data_records) == 0):
                break
            dict_df = {col:[] for col in columns}
            for record in data_records:
                for idx, column in enumerate(columns):
                    dict_df[column].append(record[idx])
            pd.DataFrame(dict_df).to_csv(output_csv, mode=mode, header=include_header, index=False)
            # reset the csv
            include_header = False
            mode = 'a'
            pbar.update(len(data_records))
    
    print(f"Converting {table_name} to csv file completed.")
        