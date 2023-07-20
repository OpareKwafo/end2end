from pathlib import Path
import os
import pandas as pd
import mysql.connector as mysql
from dotenv import load_dotenv
from src.utils import db_connection
from aws import upload_to_s3


def run_sql_script(database: str, sql_script_path: Path) -> pd.DataFrame:
    
    # read my credentials
    env_path = Path(".env")
    load_dotenv(env_path)
    
    # connect to sql
    connection, cursor = db_connection(database)
    
    # now open the sql file and grab query
    with open(sql_script_path, 'r') as file:
        sql_query = file.read()
    
    # execute the sql query
    cursor.execute(sql_query)
    cursor.fetchall()
    
    # fetch results to dataframe
    df = pd.read_sql_query(sql_query, connection)
    
    #close connection
    connection.close()
    
    return df    
    
def process() -> pd.DataFrame:
    
    database = "processed_data"
    sql_script_path = Path("src\inventory_data.sql")
    df = run_sql_script(database, sql_script_path)
    upload_to_s3(df, "merged_data.csv")
    return None