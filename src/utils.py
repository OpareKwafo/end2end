import logging
import pandas as pd
import mysql.connector as mysql
from typing import Tuple
import os
from dotenv import load_dotenv

load_dotenv('.env')

logging.basicConfig(filename='example.log',
                    format='%(asctime)s:%(levelname)s:%(message)s',
                    level=logging.INFO)

def connect_db(host: str, user: str) -> Tuple[mysql.connection.MySQLConnection, mysql.cursor.MySQLCursor]:
    """_summary_
    
    Args:
        host (str): _description_
        user (str): _description_
        
    Returns:
        Tuple[mysql.connection.MySQLConnection, mysql.cursor.MySQLCursor]: _description_
    """
    if not host:
        raise ValueError('missing required arg: host')
    if not user:
        raise ValueError('missing required arg: user')
    
    try:
        #connect to mysql server
        mydb = mysql.connect(
            host=host,
            user=user,
            password=os.getenv('PASSWORD'))
        
        #create cursor object
        mycursor = mydb.cursor()
        logging.info('Connected to MySQL')
    except Exception as e:
        raise ConnectionError(f"error: {e}")
    else:
        print("Successfully connected to server")
    return mydb, mycursor

mydb, cur = server_connect(host='localhost', user='root')

def create_db(cursor: str, database_name: str):
    
    cursor.execute(f'DROP DATABASE IF EXISTS {database_name}')
    logging.info('dropped database')
    
    query = f"CREATE DATABASE {database_name}"
    cursor.execute(query)
    logging.info("created database")
    
    cursor.execute("SHOW DATABASES")
    database = cursor.fetchall()
    return None


def create_table_in_sql():
    cursor = server_connect()[1]
    cursor.execute("USE mydatabase")
    cursor.execute("DROP TABLE IF EXISTS customers")
    return cursor.execute("CREATE TABLE customers \
                    (name VARCHAR(255), address VARCHAR(255))")