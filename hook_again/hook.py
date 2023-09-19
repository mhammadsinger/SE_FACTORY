from database_handler import execute_query, create_connection, close_connection,return_data_as_df, return_insert_into_sql_statement_from_df
from lookups import InputTypes, IncrementalField, SourceName,HookError,ErrorHandling
import datetime
from prehook import return_tables_by_schema, return_lookup_items_as_dict
from logging_handler import show_error_message
def create_etl_checkpoint(db_session):

    return_val = ErrorHandling.NO_ERROR

    try:
        query = """
            CREATE TABLE IF NOT EXISTS dw_reporting.etl_checkpoint
            (
                etl_last_run_date TIMESTAMP
            )
            """
        execute_query(db_session, query)
    except Exception as explanation:
        explanation=str(explanation)
        reason=HookError.create_etl_checkpoint.value
        show_error_message(reason,explanation)
        
    finally:
        return return_val



    
def insert_or_update_etl_checkpoint(db_session, etl_date = None):
    pass
    # update watermark
    # last_rental_date = str(df_rental['rental_date'].max())
    # if len(df_rental) > 0:
    #     update_stmnt = f"UPDATE public.etl_index SET etl_last_run_date = '{last_rental_date}'"
    #     database_handler.execute_query(db_session, update_stmnt)

def read_source_df_insert_dest(db_session, source_name, etl_date):
    try:
        source_name = source_name.value
        tables = return_tables_by_schema(source_name)
        incremental_date_dict = return_lookup_items_as_dict(IncrementalField)

        for table in tables:
            staging_query = f"""
                    SELECT * FROM {source_name}.{table} WHERE {incremental_date_dict.get(table)} >= '{etl_date}'
            """ 
            staging_df = return_data_as_df(db_session= db_session, input_type= InputTypes.SQL, file_executor= staging_query)
            dst_table = f"stg_{source_name}_{table}"
            insert_stmt = return_insert_into_sql_statement_from_df(staging_df, 'dw_reporting', dst_table)
            execute_query(db_session=db_session, query= insert_stmt)
    except Exception as error:
        return staging_query
    
def return_etl_last_updated_date(db_session):

        try:   
            query = "SELECT etl_last_run_date FROM dw_reporting.etl_checkpoint ORDER BY etl_last_run_date DESC LIMIT 1"
            etl_df = return_data_as_df(
                file_executor= query,
                input_type= InputTypes.SQL,
                db_session= db_session
            )
            if len(etl_df) == 0:
                # choose oldest day possible.
                return_date = datetime.datetime(1992,6,19)
            else:
                return_date = etl_df['etl_last_run_date'].iloc[0]
            return return_date
        except Exception as e:
            explanation = str(e)
            reason= HookError.return_etl_last_updated_date.value
            show_error_message(reason,explanation)
        

def execute_hook():


    db_session = create_connection()
    create_etl_checkpoint(db_session)
    etl_date = return_etl_last_updated_date(db_session)
    read_source_df_insert_dest(db_session,SourceName.DVD_RENTAL, etl_date)


    # start applying transformation 
    # build dimensions.
    # build facts.
    # build aggregates.
    
    close_connection(db_session)
