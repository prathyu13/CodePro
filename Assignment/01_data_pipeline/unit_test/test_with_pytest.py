##############################################################################
# Import the necessary modules
##############################################################################

import pytest
import pandas as pd
import sqlite3
from utils import *
from constants import *

###############################################################################
# Write test cases for load_data_into_db() function
# ##############################################################################

@pytest.fixture(scope='module')
def db_connections():
    """Fixture to provide database connections."""
    # Paths
    db_file_path = f"{DB_PATH}/{DB_FILE_NAME}"
    unit_test_db_file_path = f"{DB_PATH}/{UNIT_TEST_DB_FILE_NAME}"
    
    # Connect to the main database and test database
    conn = sqlite3.connect(db_file_path)
    conn_test = sqlite3.connect(unit_test_db_file_path)
    
    yield conn, conn_test
    
    # Close connections
    conn.close()
    conn_test.close()
    
def test_load_data_into_db(db_connections):
    """_summary_
    This function checks if the load_data_into_db function is working properly by
    comparing its output with test cases provided in the db in a table named
    'loaded_data_test_case'

    INPUTS
        DB_FILE_NAME : Name of the database file 'utils_output.db'
        DB_PATH : path where the db file should be present
        UNIT_TEST_DB_FILE_NAME: Name of the test database file 'unit_test_cases.db'

    SAMPLE USAGE
        output=test_get_data()

    """
    conn, conn_test = db_connections
    
    # Run the function under test
    load_data_into_db()
    
    # Retrieve expected results from the test database
    df_test_case = pd.read_sql("SELECT * FROM loaded_data_test_case", conn_test)
    
    # Retrieve results from the main database
    df_result = pd.read_sql("SELECT * FROM loaded_data", conn)
    
    # Sort and reset index for comparison
    df_expected = df_test_case.sort_values(by='created_date').reset_index(drop=True)
    df_result = df_result.sort_values(by='created_date').reset_index(drop=True)
    
    # Compare DataFrames
    pd.testing.assert_frame_equal(df_result, df_expected, check_dtype=False)
    

###############################################################################
# Write test cases for map_city_tier() function
# ##############################################################################
def test_map_city_tier(db_connections):
    """_summary_
    This function checks if map_city_tier function is working properly by
    comparing its output with test cases provided in the db in a table named
    'city_tier_mapped_test_case'

    INPUTS
        DB_FILE_NAME : Name of the database file 'utils_output.db'
        DB_PATH : path where the db file should be present
        UNIT_TEST_DB_FILE_NAME: Name of the test database file 'unit_test_cases.db'

    SAMPLE USAGE
        output=test_map_city_tier()

    """
    conn, conn_test = db_connections
    
    # Run the function under test
    map_city_tier()
    
    # Load the test data
    df_test_case = pd.read_sql("SELECT * FROM city_tier_mapped_test_case", conn_test)
    
    # Retrieve the results from the 'loaded_data' table
    df_result = pd.read_sql("SELECT * FROM city_tier_mapped", conn)
    
    # Load expected results
    df_expected = df_test_case.sort_values(by='created_date').reset_index(drop=True)  # Assuming there's an 'id' column for sorting
    df_result = df_result.sort_values(by='created_date').reset_index(drop=True)
    
    df_expected = df_expected[df_result.columns]
    df_result = df_result[df_test_case.columns]
    
    # Compare results
    pd.testing.assert_frame_equal(df_result, df_expected, check_dtype=False)
    
    
###############################################################################
# Write test cases for map_categorical_vars() function
# ##############################################################################    
def test_map_categorical_vars(db_connections):
    """_summary_
    This function checks if map_cat_vars function is working properly by
    comparing its output with test cases provided in the db in a table named
    'categorical_variables_mapped_test_case'

    INPUTS
        DB_FILE_NAME : Name of the database file 'utils_output.db'
        DB_PATH : path where the db file should be present
        UNIT_TEST_DB_FILE_NAME: Name of the test database file 'unit_test_cases.db'
    
    SAMPLE USAGE
        output=test_map_cat_vars()

    """   
    conn, conn_test = db_connections
    
    # Run the function under test
    map_categorical_vars()
    
    # Load the test data
    df_test_case = pd.read_sql("SELECT * FROM categorical_variables_mapped_test_case", conn_test)
    
    # Retrieve the results from the 'loaded_data' table
    df_result = pd.read_sql("SELECT * FROM categorical_variables_mapped", conn)
    
    # Load expected results
    df_expected = df_test_case.sort_values(by='created_date').reset_index(drop=True)  # Assuming there's an 'id' column for sorting
    df_result = df_result.sort_values(by='created_date').reset_index(drop=True)
    
    df_expected = df_expected[df_result.columns]
    df_result = df_result[df_test_case.columns]
    
    # Compare results
    pd.testing.assert_frame_equal(df_result, df_expected, check_dtype=False)
    

###############################################################################
# Write test cases for interactions_mapping() function
# ##############################################################################    
def test_interactions_mapping(db_connections):
    """_summary_
    This function checks if test_column_mapping function is working properly by
    comparing its output with test cases provided in the db in a table named
    'interactions_mapped_test_case'

    INPUTS
        DB_FILE_NAME : Name of the database file 'utils_output.db'
        DB_PATH : path where the db file should be present
        UNIT_TEST_DB_FILE_NAME: Name of the test database file 'unit_test_cases.db'

    SAMPLE USAGE
        output=test_column_mapping()

    """ 
    conn, conn_test = db_connections
    
    # Run the function under test
    interactions_mapping()
    
    # Load the test data
    df_test_case = pd.read_sql("SELECT * FROM interactions_mapped_test_case", conn_test)
    
    # Retrieve the results from the 'loaded_data' table
    df_result = pd.read_sql("SELECT * FROM interactions_mapped", conn)
    
    
    common_columns = df_result.columns.intersection(df_test_case.columns)
    
    # Load expected results
    df_expected = df_test_case[common_columns].sort_values(by='created_date').reset_index(drop=True)  # Assuming there's an 'id' column for sorting
    df_result = df_result[common_columns].sort_values(by='created_date').reset_index(drop=True)
    
    df_expected = df_expected[df_result.columns]
    df_result = df_result[df_test_case.columns]
    
    # Compare results
    pd.testing.assert_frame_equal(df_result.sort_index(axis=1), df_expected.sort_index(axis=1), check_dtype=False)

