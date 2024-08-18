##############################################################################
# Import necessary modules
# #############################################################################


from airflow import DAG
from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta
import utils
import data_validation_checks


###############################################################################
# Define default arguments and DAG
###############################################################################

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024,8,18),
    'retries' : 1, 
    'retry_delay' : timedelta(seconds=5)
}


ML_data_cleaning_dag = DAG(
                dag_id = 'Lead_Scoring_Data_Engineering_Pipeline',
                default_args = default_args,
                description = 'DAG to run data pipeline for lead scoring',
                schedule_interval = '@daily',
                catchup = False
)

###############################################################################
# Create a task for build_dbs() function with task_id 'building_db'
###############################################################################
op_build_dbs = PythonOperator(task_id='build_dbs',
                              python_callable=utils.build_dbs,
                              op_kwargs={},
                              dag=dag)

###############################################################################
# Create a task for raw_data_schema_check() function with task_id 'checking_raw_data_schema'
###############################################################################
op_check_raw_data_schema = PythonOperator(task_id='check_raw_data_schema',
                                          python_callable=data_validation_checks.raw_data_schema_check,
                                          op_kwargs={},
                                          dag=dag)

###############################################################################
# Create a task for load_data_into_db() function with task_id 'loading_data'
##############################################################################
op_load_data_into_db = PythonOperator(task_id='load_data_into_db',
                              python_callable=utils.load_data_into_db,
                              op_kwargs={},
                              dag=dag)

###############################################################################
# Create a task for map_city_tier() function with task_id 'mapping_city_tier'
###############################################################################
op_map_city_tier = PythonOperator(task_id='map_city_tier',
                              python_callable=utils.map_city_tier,
                              op_kwargs={},
                              dag=dag)

###############################################################################
# Create a task for map_categorical_vars() function with task_id 'mapping_categorical_vars'
###############################################################################
op_map_categorical_vars = PythonOperator(task_id='map_categorical_vars',
                              python_callable=utils.map_categorical_vars,
                              op_kwargs={},
                              dag=dag)

###############################################################################
# Create a task for interactions_mapping() function with task_id 'mapping_interactions'
###############################################################################
op_interactions_mapping = PythonOperator(task_id='interactions_mapping',
                              python_callable=utils.interactions_mapping,
                              op_kwargs={},
                              dag=dag)

###############################################################################
# Create a task for model_input_schema_check() function with task_id 'checking_model_inputs_schema'
###############################################################################
op_check_model_input_schema = PythonOperator(task_id='check_model_input_schema',
                                          python_callable=data_validation_checks.model_input_schema_check,
                                          op_kwargs={},
                                          dag=dag)

###############################################################################
# Define the relation between the tasks
###############################################################################


