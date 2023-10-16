from database_handler import execute_query,create_connection, return_data_as_df,close_connection
from lookups import *
from logging_handler import show_error_message
from misc_handler import get_prehook_sql_files,return_currencies_to_replicate,insert_into_etl_logging_table
import os
import pandas as pd
import datetime
def execute_prehook_in_sql_folder(db_session, sql_command_directory_path, target_schema):
    sql_files = get_prehook_sql_files(sql_command_directory_path)
    sorted_sql_files =  sorted(sql_files)
    for sql_file in sorted_sql_files:
            with open(os.path.join(sql_command_directory_path,sql_file), 'r') as file:
                sql_query = file.read()
                sql_query = sql_query.replace('target_schema', target_schema.value)
                return_val = execute_query(db_session= db_session, query= sql_query)

                if not return_val == ErrorHandling.NO_ERROR:
                    raise Exception(f"Error in function execute_prehook_in_sql_folder in prehook.py, SQL File Error on SQL FILE = " +  str(sql_file))




def return_create_statement_from_df(dataframe,schema_name, table_name):
    type_mapping = {
        'int64':'INT',
        'float64':'FLOAT',
        'datetime64[ns]': 'TIMESTAMP',
        'bool':'BOOLEAN',
        'object': 'TEXT'
    }
    fields = []
    for column, dtype in dataframe.dtypes.items():
        sql_type = type_mapping.get(str(dtype), 'TEXT')
        fields.append(f"{column} {sql_type}")
    
    create_table_statemnt = f"CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (\n"
    create_table_statemnt += ",\n".join(fields)
    create_table_statemnt += "\n);"
    return create_table_statemnt


def create_sql_staging_tables(db_session):

    try:
        all_currencies = return_currencies_to_replicate()
        
        for each_currency in all_currencies:

            staging_df = pd.DataFrame(columns=['Datetime', 'close'])
            
            staging_df['Datetime']= pd.to_datetime(staging_df['Datetime'])
            #columns = list(staging_df.columns)
            dst_table = f"{each_currency}"

            create_stmt = return_create_statement_from_df(staging_df, 'stg_schema', dst_table[:-2])

            execute_query(db_session=db_session, query= create_stmt)
            #create_sql_staging_table_index(db_session, 'dw_reporting', dst_table, columns[0])
    except Exception as error:

        reason=ErrorHandling.PREHOOK_ERROR_CREATING_TABLES.value
        explanation=str(error)
        show_error_message(reason, explanation)
    
    
    
def execute_prehook(sql_command_directory_path = './SQL_Commands'):
    step_name = "execute_prehook"

    try:
        db_session = create_connection()

        start_time = datetime.datetime.now()
        
        create_sql_staging_tables(db_session)
        execute_prehook_in_sql_folder(db_session, sql_command_directory_path, DestinationName.STAGING_SCHEMA) 

        end_time = datetime.datetime.now()

        insert_into_etl_logging_table( db_session, DestinationName.STAGING_SCHEMA, PreHookSteps.CREATE_SQL_STAGING, start_time, end_time)
        close_connection(db_session)
        
    except Exception as error:
        suffix = str(error)
        error_prefix = ErrorHandling.PREHOOK_SQL_ERROR
        show_error_message(error_prefix.value, suffix)
        raise Exception(f"Important Step Failed step = {step_name}")