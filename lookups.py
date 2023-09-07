from enum import Enum
from logging_handler import show_error_message


class ErrorHandling(Enum):

    try:
        DB_CONNECT_ERROR = "DB could not connect"
        DB_RETURN_QUERY_AS_DF_ERROR = 'query could not be returned as dataframe'
        DB_EXECUTE_QUERY= 'Execution error'
        DB_schema_info='Error getting schema tables'
        DB_DROP_NULLS=' Error droping nulls'
        RETURN_DATA_SQL_ERROR = "Error returning SQL"
        RETURN_DATA_CSV_ERROR = "Error returning CSV"
        RETURN_GET_SHAPE_SQL_ERROR_ = ' Error finding shape from query'
        RETURN_GET_SHAPE_CSV_ERROR_ = ' Error finding CSV shape'
        RETURN_DROP_DUPLICATES_SQL_ERROR_= 'ERROR droping duplicates from query'
        RETURN_DROP_DUPLICATES_CSV_ERROR_ = 'Error droping duplicates from csv file'
        RETURN_GET_LENGTH_SQL_ERROR= ' Error finding length from query'
        RETURN_GET_LENGTH_CSV_ERROR_ = ' Error finding CSV length'




    
    except Exception as reading_error:

            reason='Error handling enums '
            explanation=str(reading_error)
            show_error_message(reason,explanation)        


class InputTypes(Enum):
    SQL = "SQL"
    CSV = "CSV"
    EXCEL = "Excel"
    