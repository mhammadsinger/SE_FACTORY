import psycopg2
import pandas as pd
from lookups import ErrorHandling,InputTypes
from logging_handler import show_error_message
import os


def create_connection(database,user,password,host,port):
    
    try:
        db_session = psycopg2.connect(database = database,user = user,password = password,host = host,port = port)
        return db_session
    #except OperationalError:
            #pass
    except  Exception as connection_error:
            
        reason = ErrorHandling.DB_CONNECT_ERROR.value
        explanation = str(connection_error)
        show_error_message(reason, explanation)

    return 'create_connection funtion returned nothing'



def return_query_as_df(db_session,query):

    try:
        query_df = pd.read_sql_query(sql= query, con=db_session)
        return query_df
    except Exception as reading_error:
        reason=ErrorHandling.DB_RETURN_QUERY_AS_DF_ERROR.value
        explanation=str(reading_error)
        show_error_message(reason, explanation)


def execute_query(db_session,query):

    try:
        cursor = db_session.cursor()
        cursor.execute(query)
        db_session.commit()

    except Exception as executing_error:
        reason = ErrorHandling.DB_EXECUTE_QUERY.value
        explanation = str(executing_error)
        show_error_message(reason, explanation)

