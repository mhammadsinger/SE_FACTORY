from enum import Enum


class ErrorHandling(Enum):
    
    DB_CONNECT_ERROR = "DB Connect Error"
    DB_RETURN_QUERY_ERROR = "DB Return Query Error"
    POSTHOOK_ERROR_TRUNCATE=' Error truncating staging tables'
    RETURN_DATA_SQL_ERROR = "Error returning SQL"
    PREHOOK_ERROR_CREATING_TABLES= 'Error creating sql table'
    EXECUTE_QUERY_ERROR = "Error executing the query"
    NO_ERROR = "No Errors"
    PREHOOK_SQL_ERROR = "Prehook: SQL Error"
    HOOK_SQL_ERROR = "hook: SQL Error"

    PREHOOK_ERROR_INSERTING = "error in function read_source_df_insert_dest"
class CurrenciesToReplicate(Enum):
    EURUSD = "EURUSD=X"
    gbpjpy = "GBPJPY=X"
    gbpusd = "GBPUSD=X"
    
class InputTypes(Enum):
    SQL = "SQL"
    CSV = "CSV"
    EXCEL = "Excel"


class DestinationName(Enum):
    STAGING_SCHEMA = "stg_schema"
    TARGET_SCHEMA = "currencies_schema"


class PreHookSteps(Enum):
    EXECUTE_SQL_QUERY = "execute_sql_folder"
    CREATE_SQL_STAGING = "create_sql_staging_tables"

class HookSteps(Enum):
    EXECUTE_SQL_QUERY = "execute_sql_folder"
    CREATE_SQL_STAGING = "create_sql_staging_tables"

class ETLStep(Enum):
    PRE_HOOK = 'prehook'
    HOOK = 'hook'
    POST_HOOK = 'posthook'
