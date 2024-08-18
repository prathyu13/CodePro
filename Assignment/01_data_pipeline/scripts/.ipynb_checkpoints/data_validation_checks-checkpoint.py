"""
Import necessary modules
############################################################################## 
"""

import pandas as pd
from schema import *
from constants import *
import os
import sqlite3

###############################################################################
# Define function to validate raw data's schema
############################################################################### 

def raw_data_schema_check():
    '''
    This function check if all the columns mentioned in schema.py are present in
    leadscoring.csv file or not.

   
    INPUTS
        DATA_DIRECTORY : path of the directory where 'leadscoring.csv' 
                        file is present
        raw_data_schema : schema of raw data in the form oa list/tuple as present 
                          in 'schema.py'

    OUTPUT
        If the schema is in line then prints 
        'Raw datas schema is in line with the schema present in schema.py' 
        else prints
        'Raw datas schema is NOT in line with the schema present in schema.py'

    
    SAMPLE USAGE
        raw_data_schema_check
    '''
    csv_file_path = f"{DATA_DIRECTORY}/leadscoring.csv"

    try:
        df = pd.read_csv(csv_file_path)
    except FileNotFoundError:
        print(f"File {csv_file_path} not found.")
        return
    except pd.errors.EmptyDataError:
        print("The CSV file is empty.")
        return
    except pd.errors.ParserError:
        print("Error parsing the CSV file.")
        return

    csv_columns = set(df.columns)

    # Compare with the schema
    schema_columns = set(raw_data_schema)

    if schema_columns.issubset(csv_columns):
        print('Raw data schema is in line with the schema present in schema.py')
    else:
        print('Raw data schema is NOT in line with the schema present in schema.py')

   
###############################################################################
# Define function to validate model's input schema
############################################################################### 

def model_input_schema_check():
    '''
    This function check if all the columns mentioned in model_input_schema in 
    schema.py are present in table named in 'model_input' in db file.

   
    INPUTS
        DB_FILE_NAME : Name of the database file
        DB_PATH : path where the db file should be present
        model_input_schema : schema of models input data in the form oa list/tuple
                          present as in 'schema.py'

    OUTPUT
        If the schema is in line then prints 
        'Models input schema is in line with the schema present in schema.py'
        else prints
        'Models input schema is NOT in line with the schema present in schema.py'
    
    SAMPLE USAGE
        raw_data_schema_check
    '''
    db_file_path = os.path.join(DB_PATH, DB_FILE_NAME)
    
    if not os.path.exists(db_file_path):
        print(f"Database file {db_file_path} not found.")
        return
    
    try:
        conn = sqlite3.connect(db_file_path)
        cursor = conn.cursor()
        
        table_name = 'model_input' 
        
        # Query to get the column names of the table
        cursor.execute(f"PRAGMA table_info({table_name})")
        columns_info = cursor.fetchall()
        
        # Extract column names
        db_columns = set(col_info[1] for col_info in columns_info)  # col_info[1] is the column name
        
        # Compare with the schema
        schema_columns = set(model_input_schema)
        
        if schema_columns.issubset(db_columns):
            print('Models input schema is in line with the schema present in schema.py')
        else:
            print('Models input schema is NOT in line with the schema present in schema.py')
    
    except sqlite3.Error as e:
        print(f"SQLite error: {e}")
    finally:
        # Close the connection
        if conn:
            conn.close()
    

    
    
