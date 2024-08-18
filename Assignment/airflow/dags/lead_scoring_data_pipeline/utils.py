##############################################################################
# Import necessary modules and files
##############################################################################


import pandas as pd
import os
import sqlite3
from sqlite3 import Error
from constants import *
from mapping.significant_categorical_level import *
from mapping.city_tier_mapping import city_tier_mapping
###############################################################################
# Define the function to build database
###############################################################################

def build_dbs():
    '''
    This function checks if the db file with specified name is present 
    in the /Assignment/01_data_pipeline/scripts folder. If it is not present it creates 
    the db file with the given name at the given path. 


    INPUTS
        DB_FILE_NAME : Name of the database file 'lead_scoring_data_cleaning.db'
        DB_PATH : path where the db file should exist  


    OUTPUT
    The function returns the following under the given conditions:
        1. If the file exists at the specified path
                prints 'DB Already Exists' and returns 'DB Exists'

        2. If the db file is not present at the specified loction
                prints 'Creating Database' and creates the sqlite db 
                file at the specified path with the specified name and 
                once the db file is created prints 'New DB Created' and 
                returns 'DB created'


    SAMPLE USAGE
        build_dbs()
    '''
    print(DB_PATH)
    if os.path.isfile(DB_PATH+DB_FILE_NAME):
        print( "DB Already Exsist")
        print(os.getcwd())
        return "DB Exsist"
    else:
        print ("Creating Database")
        """ create a database connection to a SQLite database """
        conn = None
        try:
            
            conn = sqlite3.connect(DB_PATH+DB_FILE_NAME)
            print("New DB Created")
        except Error as e:
            print(e)
            return "Error"
        finally:
            if conn:
                conn.close()
                return "DB Created"

###############################################################################
# Define function to load the csv file to the database
###############################################################################

def load_data_into_db():
    '''
    Thie function loads the data present in data directory into the db
    which was created previously.
    It also replaces any null values present in 'toal_leads_dropped' and
    'referred_lead' columns with 0.


    INPUTS
        DB_FILE_NAME : Name of the database file
        DB_PATH : path where the db file should be
        DATA_DIRECTORY : path of the directory where 'leadscoring.csv' 
                        file is present
        

    OUTPUT
        Saves the processed dataframe in the db in a table named 'loaded_data'.
        If the table with the same name already exsists then the function 
        replaces it.


    SAMPLE USAGE
        load_data_into_db()
    '''
    db_file_path = f"{DB_PATH}/{DB_FILE_NAME}"
    
    data_file_path = f"{DATA_DIRECTORY}/leadscoring.csv"
    df = pd.read_csv(data_file_path)
    
    if 'total_leads_dropped' in df.columns:
        df['total_leads_dropped'] = df['total_leads_dropped'].fillna(0)
    
    if 'referred_lead' in df.columns:
        df['referred_lead'] = df['referred_lead'].fillna(0)
    
    conn = sqlite3.connect(db_file_path)
    
    df.to_sql('loaded_data', con=conn, if_exists='replace', index=False)
    
    conn.commit()
    conn.close()

    print(f"Data loaded into the database at {db_file_path} in the 'loaded_data' table.")
    

###############################################################################
# Define function to map cities to their respective tiers
###############################################################################

    
def map_city_tier():
    '''
    This function maps all the cities to their respective tier as per the
    mappings provided in the city_tier_mapping.py file. If a
    particular city's tier isn't mapped(present) in the city_tier_mapping.py 
    file then the function maps that particular city to 3.0 which represents
    tier-3.


    INPUTS
        DB_FILE_NAME : Name of the database file
        DB_PATH : path where the db file should be
        city_tier_mapping : a dictionary that maps the cities to their tier

    
    OUTPUT
        Saves the processed dataframe in the db in a table named
        'city_tier_mapped'. If the table with the same name already 
        exsists then the function replaces it.

    
    SAMPLE USAGE
        map_city_tier()

    '''
    db_file_path = f"{DB_PATH}/{DB_FILE_NAME}"
    
    conn = sqlite3.connect(db_file_path)
    
    query = "SELECT * FROM loaded_data"
    df = pd.read_sql(query, conn)
    
    df['city_tier'] = df['city_mapped'].map(city_tier_mapping)
    
    # Fill any missing values with 3.0 (default tier)
    df['city_tier'] = df['city_tier'].fillna(3.0)
    
    df = df.drop(['city_mapped'], axis = 1)
    
    df.to_sql('city_tier_mapped', con=conn, if_exists='replace', index=False)
    
    conn.commit()
    conn.close()

    print(f"Data with city tiers mapped has been saved to the database at {db_file_path} in the 'city_tier_mapped' table.")

###############################################################################
# Define function to map insignificant categorial variables to "others"
###############################################################################


def map_categorical_vars():
    '''
    This function maps all the insignificant variables present in 'first_platform_c'
    'first_utm_medium_c' and 'first_utm_source_c'. The list of significant variables
    should be stored in a python file in the 'significant_categorical_level.py' 
    so that it can be imported as a variable in utils file.
    

    INPUTS
        DB_FILE_NAME : Name of the database file
        DB_PATH : path where the db file should be present
        list_platform : list of all the significant platform.
        list_medium : list of all the significat medium
        list_source : list of all rhe significant source

        **NOTE : list_platform, list_medium & list_source are all constants and
                 must be stored in 'significant_categorical_level.py'
                 file. The significant levels are calculated by taking top 90
                 percentils of all the levels. For more information refer
                 'data_cleaning.ipynb' notebook.
  

    OUTPUT
        Saves the processed dataframe in the db in a table named
        'categorical_variables_mapped'. If the table with the same name already 
        exsists then the function replaces it.

    
    SAMPLE USAGE
        map_categorical_vars()
    '''
    
    db_file_path = f"{DB_PATH}/{DB_FILE_NAME}"

    conn = sqlite3.connect(db_file_path)
    
    query = "SELECT * FROM city_tier_mapped"
    df = pd.read_sql(query, conn)
    
    # Define a mapping function
    def map_to_significant_levels(column, significant_levels):
        """
        Map the column values to significant levels, replacing insignificant levels
        with 'Other'.
        """
        df[column] = df[column].apply(lambda x: x if x in significant_levels else 'others')
    
    # Map each categorical variable using the defined lists
    map_to_significant_levels('first_platform_c', list_platform)
    map_to_significant_levels('first_utm_medium_c', list_medium)
    map_to_significant_levels('first_utm_source_c', list_source)
    
    # Save the processed DataFrame to the database
    df.to_sql('categorical_variables_mapped', con=conn, if_exists='replace', index=False)
    
    # Commit changes and close the connection
    conn.commit()
    conn.close()

    print(f"Data with categorical variables mapped has been saved to the database at {db_file_path} in the 'categorical_variables_mapped' table.")



##############################################################################
# Define function that maps interaction columns into 4 types of interactions
##############################################################################
def interactions_mapping():
    '''
    This function maps the interaction columns into 4 unique interaction columns
    These mappings are present in 'interaction_mapping.csv' file. 


    INPUTS
        DB_FILE_NAME: Name of the database file
        DB_PATH : path where the db file should be present
        INTERACTION_MAPPING : path to the csv file containing interaction's
                                   mappings
        INDEX_COLUMNS_TRAINING : list of columns to be used as index while pivoting and
                                 unpivoting during training
        INDEX_COLUMNS_INFERENCE: list of columns to be used as index while pivoting and
                                 unpivoting during inference
        NOT_FEATURES: Features which have less significance and needs to be dropped
                                 
        NOTE : Since while inference we will not have 'app_complete_flag' which is
        our label, we will have to exculde it from our features list. It is recommended 
        that you use an if loop and check if 'app_complete_flag' is present in 
        'categorical_variables_mapped' table and if it is present pass a list with 
        'app_complete_flag' column, or else pass a list without 'app_complete_flag'
        column.

    
    OUTPUT
        Saves the processed dataframe in the db in a table named 
        'interactions_mapped'. If the table with the same name already exsists then 
        the function replaces it.
        
        It also drops all the features that are not requried for training model and 
        writes it in a table named 'model_input'

    
    SAMPLE USAGE
        interactions_mapping()
    '''
    db_file_path = f"{DB_PATH}/{DB_FILE_NAME}"
    
    conn = sqlite3.connect(db_file_path)
    
    df_event_mapping = pd.read_csv(INTERACTION_MAPPING, index_col=0)
    
    # Load the data from the database
    query = "SELECT * FROM categorical_variables_mapped"
    df = pd.read_sql(query, conn)
    
    df = df.drop_duplicates()
    
    df_unpivot = pd.melt(df, id_vars=INDEX_COLUMNS_TRAINING, var_name='interaction_type', value_name='interaction_value')
    
    # Fill NaN values in interaction_value column
    df_unpivot['interaction_value'] = df_unpivot['interaction_value'].fillna(0)
    
    # Merge with the interaction mapping
    df = pd.merge(df_unpivot, df_event_mapping, on='interaction_type', how='left')
    
    # Drop the original interaction_type column
    df = df.drop(['interaction_type'], axis=1)
    
    # Pivot the DataFrame to aggregate interaction values
    df_pivot = df.pivot_table(
        values='interaction_value', 
        index=INDEX_COLUMNS_TRAINING, 
        columns='interaction_mapping', 
        aggfunc='sum'
    )
    
    # Reset index to flatten the DataFrame
    df_pivot = df_pivot.reset_index()
    
    # Drop columns that are not needed for the model
    features_to_drop = [col for col in df_pivot.columns if col in NOT_FEATURES]
    df_pivot = df_pivot.drop(columns=features_to_drop, errors='ignore')
    
    # Save the processed DataFrame to the database
    df_pivot.to_sql('interactions_mapped', con=conn, if_exists='replace', index=False)
    
    # Save the model input DataFrame
    model_input_columns = [col for col in df_pivot.columns if col not in INDEX_COLUMNS_TRAINING]
    df_model_input = df_pivot[INDEX_COLUMNS_TRAINING + model_input_columns]
    df_model_input.to_sql('model_input', con=conn, if_exists='replace', index=False)
    
    # Commit changes and close the connection
    conn.commit()
    conn.close()
    
    print(f"Interaction columns have been mapped and saved to 'interactions_mapped'. Model input features saved to 'model_input'.")
