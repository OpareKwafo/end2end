import src.utils
# from src.utils import *
# from src.utils import db_connection, create_database, create_table_in_sql, read_data, insert_data
# from utils import db_connection, create_database


connection, cursor = db_connection()

db = False

if db:
    create_database(database_name= "")
else:
    df = read_data(filepath)
    col_type, values = python_df_to_sql_table(df)
    create_table_in_sql(database_name, table_name, col_type)
    insert_data(df, table_name, values)
    
cursor.close()
connection.close()
    
    



