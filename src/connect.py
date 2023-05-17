import mysql.connector as mysql
from dotenv import load_dotenv
import os
import pandas as pd

load_dotenv('.env')

# FUNCTION TO CONNECT TO MYSQL SERVER
def server_connect():
    mydb = mysql.connect(
        host='localhost', 
        user='root', 
        password=os.getenv('PASSWORD')
        )
    
    mycursor = mydb.cursor()
    return [mydb, mycursor]


# FUNCTION TO CREATE DATABASE
def create_db():
    cursor = server_connect()[1]
    cursor.execute("DROP DATABASE IF EXISTS mydatabase")
    query = "CREATE DATABASE mydatabase"
    return cursor.execute(query)


#CREATE TABLE WITHIN A DATABASE
#drop table
def create_table():
    cursor = server_connect()[1]
    cursor.execute("USE mydatabase")
    cursor.execute("DROP TABLE IF EXISTS customers")
    return cursor.execute("CREATE TABLE customers \
                    (name VARCHAR(255), address VARCHAR(255))")

def read_file(path_to_csv):
    df = pd.read_csv(path_to_csv)
    df = df.loc[:, ~df.columns.str.contains('^Unamed')]
    return df
    


create_db()
    
#CLOSE CONNECTION
server_connect()[1].close()
server_connect()[0].close()