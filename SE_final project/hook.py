from database_handler import create_connection, return_data_as_df,return_insert_into_sql_statement_from_df,execute_query, close_connection
from lookups import InputTypes,CurrenciesToReplicate, ErrorHandling,DestinationName,HookSteps
import datetime
import pandas as pd
import yfinance as yf
from datetime import datetime,timezone
import pytz
from misc_handler import get_hook_sql_files
import os
from logging_handler import show_error_message            
from misc_handler import insert_into_etl_logging_table

def current_time():
    timezone= pytz.timezone('Europe/London')
    time=datetime.now(timezone)
    return time.replace(microsecond=0)

def execute_hook_in_sql_folder(db_session, sql_command_directory_path, target_schema):
    sql_files = get_hook_sql_files(sql_command_directory_path)
    sorted_sql_files =  sorted(sql_files)
    for sql_file in sorted_sql_files:
            with open(os.path.join(sql_command_directory_path,sql_file), 'r') as file:
                sql_query = file.read()
                sql_query = sql_query.replace('target_schema', target_schema.value)
                return_val = execute_query(db_session= db_session, query= sql_query)

                if not return_val == ErrorHandling.NO_ERROR:
                    raise Exception(f"Error in function execute_prehook_in_sql_folder in prehook.py, SQL File Error on SQL FILE = " +  str(sql_file))
                
def read_source_df_insert_dest(db_session, etl_date, time_right_now ):
    result='There was an error while retreiving data'

    try:
        
        all_currencies = [table.value for table in CurrenciesToReplicate]
        
        for currency in all_currencies:

            staging_df = yf.download(currency, start=etl_date, end=time_right_now, interval="5m")

            staging_df=staging_df.reset_index()

            staging_df = staging_df[['Datetime','Close']]
            staging_df['Datetime']= pd.to_datetime(staging_df['Datetime'])   

            dst_table = f"{currency[:-2]}"
            insert_stmt = return_insert_into_sql_statement_from_df(staging_df, 'stg_schema', dst_table)

            for statement in insert_stmt:
                execute_query(db_session=db_session, query= statement)   

        result=ErrorHandling.NO_ERROR
    except Exception as error:
        reason=ErrorHandling.PREHOOK_ERROR_INSERTING.value
        explanation=str(error)
        show_error_message(reason, explanation)
    
    finally:
        return result

    

def return_etl_last_updated_date(db_session):

    query = "SELECT etl_last_run_date FROM stg_schema.etl_checkpoint ORDER BY etl_last_run_date DESC LIMIT 1"
    etl_df = return_data_as_df(
        file_executor= query,
        input_type= InputTypes.SQL,
        db_session= db_session
    )
    if len(etl_df) == 0:

        return_date = datetime(2023,9,24)
    else:
        return_date = etl_df['etl_last_run_date'].iloc[0]
        
    return return_date





def insert_or_update_etl_checkpoint(db_session, time_right_now):


        insert_stmt=f"INSERT INTO stg_schema.etl_checkpoint(etl_last_run_date) VALUES ('{time_right_now}')"

        execute_query(db_session=db_session, query= insert_stmt) 


def execute_hook(sql_command_directory_path = './SQL_Commands'):
    step_name = "execute_hook"

    try:
             
        db_session = create_connection()


        start_time =datetime.now()
        
        time_right_now = current_time()
        etl_date = return_etl_last_updated_date(db_session)

        
        read_source_df_insert_dest(db_session,etl_date,time_right_now)

        execute_hook_in_sql_folder(db_session, sql_command_directory_path, DestinationName.TARGET_SCHEMA)    

        insert_or_update_etl_checkpoint(db_session, time_right_now)
        
        end_time = datetime.now()

        insert_into_etl_logging_table( db_session, DestinationName.TARGET_SCHEMA, HookSteps.CREATE_SQL_STAGING, start_time, end_time)

        close_connection(db_session)
        
    except Exception as error:    
        suffix = str(error)
        error_prefix = ErrorHandling.HOOK_SQL_ERROR
        show_error_message(error_prefix.value, suffix)
        raise Exception(f"Important Step Failed step = {step_name}")