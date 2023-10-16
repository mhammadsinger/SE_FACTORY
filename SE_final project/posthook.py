from database_handler import execute_query, create_connection
from misc_handler import return_currencies_to_replicate
from lookups import DestinationName,ErrorHandling
from logging_handler import show_error_message

def truncate_staging_tables(source_name, all_currencies, db_session):

    
    try:
        for currency in all_currencies:
            dst_table = f"{currency[:-2]}"
            truncate_query = f"""
            TRUNCATE TABLE {source_name}.{dst_table}"""
            execute_query(db_session, truncate_query)

    except Exception as error:
        reason=ErrorHandling.POSTHOOK_ERROR_TRUNCATE.value
        explanation=str(error)
        show_error_message(reason, explanation)
    




def execute_posthook():

    db_session = create_connection()
    all_currencies = return_currencies_to_replicate()

    truncate_staging_tables(DestinationName.STAGING_SCHEMA.value, all_currencies, db_session)

