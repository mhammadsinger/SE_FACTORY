
from lookups import ETLStep,CurrenciesToReplicate
from database_handler import execute_query

import os
def get_prehook_sql_files(sql_command_directory_path):
        sql_files = [sqlfile for sqlfile in os.listdir(sql_command_directory_path) if (sqlfile.endswith('.sql') and ETLStep.PRE_HOOK.value in sqlfile) ]
        return sql_files

def get_hook_sql_files(sql_command_directory_path):
        sql_files = [sqlfile for sqlfile in os.listdir(sql_command_directory_path) if (sqlfile.endswith('.sql') and ETLStep.HOOK.value in sqlfile) ]
        return sql_files


def return_currencies_to_replicate():
    currencies_tables = list()
    tables = [table.value for table in CurrenciesToReplicate]
    for table in tables:
        currencies_tables.append(table)
    return currencies_tables


def insert_into_etl_logging_table( db_session, schema_name, step_name, start_time, end_time):
    query = f"""
    INSERT INTO {schema_name.value}.etl_history (step_name, execution_start, execution_end)
    VALUES
    (
        '{step_name.value}',
        '{start_time}',
        '{end_time}'
    )
    """

    execute_query(db_session, query)