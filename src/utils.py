import logging
import pandas as pd
import mysql.connector as mysql
from typing import Tuple, List, Optional
import os
from dotenv import load_dotenv
from pathlib import Path

#FUNCTION TO CONNECT TO MYSQL DATABASE

def db_connection(database: str = None) -> Tuple[mysql.connection.MySQLConnection, str]:
    load_dotenv(".env")                 
    connection = mysql.connect(
        host = os.getenv("HOST"),
        user= os.getenv("USER"),
        password = os.getenv("PASSWORD"),
        database = None
    )

    #CREATE CURSOR
    cursor = connection.cursor()
    return connection, cursor

#FUNCTION TO CREATE DATABASE

def create_database(database_name: str, cursor: str) -> None:
    sql_query = f"DROP DATABASE IF EXISTS {database_name}"
    cursor.execute(sql_query)

    sql_query = f"CREATE DATABASE {database_name}"
    cursor.execute(sql_query)

    cursor.execute("SHOW DATABASES")
    res = cursor.fetchall()
    print(res)

#FUNCTION TO CREATE TABLE
def create_table_in_sql(database_name: str, table_name: str, col_type:str, cursor:str) -> None:
    cursor.execute(f"USE {database_name}") 
    
    #drop table if already exists
    sql_query = f"DROP TABLE IF EXISTS {table_name}"
    cursor.execute(sql_query)

    sql_query = f'CREATE TABLE {table_name} ({col_type})'
    cursor.execute(sql_query)
    
    return None


#FUNCTION TO CONVERT PANDAS DTYPES TO SQL DATA TYPES
def python_df_to_sql_table(df: pd.DataFrame) -> Tuple[str, str]:
    types = []
    for i in df.dtypes:
        if i == 'object':
            types.append('VARCHAR(255)')
        elif i == 'float64':
            types.append('FLOAT')
        elif i == 'int64':
            types.append('INT')
            
    # Combine column names and data types into a string of SQL schema
    col_type = list(zip(df.columns.values, types))
    col_type = tuple([" ".join(i) for i in col_type])
    col_type = ", ".join(col_type)
    
    # Create a string of placeholder values for SQL queries
    values = ', '.join(['%s' for _ in range(len(df.columns))])
    
    return col_type, values


#FUNCTION TO READ CSV AS PANDAS DATAFRAMES, RETURN DATAFRAMES
def read_data(file_path: str) -> pd.DataFrame:
    df = pd.read_csv(file_path)
    df.drop("Unnamed: 0", axis=1, inplace=True, errors='ignore')
    
    return df
    

#FUNCTION TO INSERT DATA INTO DATABASE
def insert_data(df: pd.DataFrame, table_name: str, values: str, cursor: str, connection: str):
    for i, row in df.iterrows():
        sql = f"INSERT INTO {table_name} VALUES ({values})"
        cursor.execute(sql, tuple(row))
        connection.commit()
    # print(cursor.rowcount)
    
    
    