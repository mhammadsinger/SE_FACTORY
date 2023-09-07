
import psycopg2
import pandas as pd
from lookups import ErrorHandling,InputTypes
from logging_handler import show_error_message
import os
from db_handler import return_query_as_df

def schema_info(db_session,schema_name):
    query=f"SELECT table_name FROM information_schema.tables WHERE table_schema='{schema_name}'"

    try:
        return return_query_as_df(db_session,query)
    
    except Exception as reading_error:
        reason=ErrorHandling.DB_schema_info.value
        explanation=str(reading_error)
        show_error_message(reason,explanation)
    
    return f'getting tables of schema {schema_name} didnt work'


def drop_nulls(db_session,data,data_type):

    try:
        if (data_type)==(InputTypes.SQL.value):
            df=return_query_as_df(db_session,query=data)
            df=df.dropna()
            
            return df
        elif (data_type)==(InputTypes.CSV.value):
            df=pd.read_csv(db_session,data)
            df=df.dropna()

    except Exception as reading_error:

        if data_type=='SQL':
            reason=ErrorHandling.RETURN_DATA_SQL_ERROR.value
            explanation=str(reading_error)
            show_error_message(reason,explanation)
        
        elif data_type=='CSV':   

            reason=ErrorHandling.RETURN_DATA_CSV_ERROR.value
            explanation=str(reading_error)
            show_error_message(reason,explanation)
        else:
            print(f'Error returning {data_type} file')
            explanation=str(reading_error)
            show_error_message(reason,explanation)            


def get_shape(db_session,data,data_type):


    try:
        if (data_type)==(InputTypes.SQL.value):
            df=return_query_as_df(db_session,query=data)
           
            return df.shape
        
        elif (data_type)==(InputTypes.CSV.value):
            df=pd.read_csv(db_session,data)

            return df.shape

    except Exception as reading_error:

        if data_type=='SQL':
            
            reason=ErrorHandling.RETURN_GET_SHAPE_SQL_ERROR_.value
            explanation=str(reading_error)
            show_error_message(reason,explanation)
        
        elif data_type=='CSV':   

            reason=ErrorHandling.RETURN_GET_SHAPE_CSV_ERROR_.value
            explanation=str(reading_error)
            show_error_message(reason,explanation)
        else:
            print(f'Error returning {data_type} file')
            explanation=str(reading_error)
            show_error_message(reason,explanation)            


def drop_duplicates(db_session,data,data_type):

    try:
        if (data_type)==(InputTypes.SQL.value):
            df=return_query_as_df(db_session,query=data)
            df=df.drop_duplicates()
            return df
        
        elif (data_type)==(InputTypes.CSV.value):
            df=pd.read_csv(db_session,data)
            df=df.drop_duplicates()
            return df
    except Exception as reading_error:

        if data_type=='SQL':
            
            reason=ErrorHandling.RETURN_DROP_DUPLICATES_SQL_ERROR_.value
            explanation=str(reading_error)
            show_error_message(reason,explanation)

        elif data_type=='CSV':
            
            reason=ErrorHandling.RETURN_DROP_DUPLICATES_CSV_ERROR_.value
            explanation=str(reading_error)
            show_error_message(reason,explanation)
        
        else:
            print(f'Error droping duplicates from {data_type} file')
            explanation=str(reading_error)
            show_error_message(reason,explanation)    


def get_length(db_session,data,data_type):


    try:
        if (data_type)==(InputTypes.SQL.value):
            df=return_query_as_df(db_session,query=data)
           
            return df.shape[0]
        
        elif (data_type)==(InputTypes.CSV.value):
            df=pd.read_csv(db_session,data)

            return df.shape[0]

    except Exception as reading_error:

        if data_type=='SQL':
            
            reason=ErrorHandling.RETURN_GET_LENGTH_SQL_ERROR.value
            explanation=str(reading_error)
            show_error_message(reason,explanation)
        
        elif data_type=='CSV':   

            reason=ErrorHandling.RETURN_GET_LENGTH_CSV_ERROR_.value
            explanation=str(reading_error)
            show_error_message(reason,explanation)
        else:
            print(f'Error returning {data_type} file')
            explanation=str(reading_error)
            show_error_message(reason,explanation) 

