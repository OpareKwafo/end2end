from utils import (db_connection, create_database, 
                       create_table_in_sql, read_data, insert_data, python_df_to_sql_table)
import argparse


parser = argparse.ArgumentParser(
                    prog='database_script',
                    description='creates db and inserts data')

parser.add_argument('-db', '--create_db_or_not', type=bool,
                    default=False)
parser.add_argument('-db_name', '--database_name',type=str,
                    required=True)
parser.add_argument('-tb_name', '--table_name', type=str,
                    required=True)
parser.add_argument('-fp', '--file_path', type=str, required=True)

args = parser.parse_args()

connection, cursor = db_connection()

# db = False
# database_name="db_example"
# table_name = "table_example"

if args.create_db_or_not:
    create_database(database_name=args.database_name, cursor=cursor)
else:
    df = read_data(file_path=args.file_path)
    col_type, values = python_df_to_sql_table(df)
    create_table_in_sql(database_name=args.database_name, table_name=args.table_name, 
                        col_type=col_type, cursor=cursor)
    insert_data(df=df, table_name=args.table_name, values=values, 
                cursor=cursor, connection=connection)
    
cursor.close()
connection.close()
    
    



