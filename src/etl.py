import pandas as pd
from datetime import datetime
from src.utils import read_data
import os

# sales = pd.read_csv("data\sales.csv", usecols=lambda column: column != 'Unnamed: 0')
# temp = pd.read_csv("data\sensor_storage_temperature.csv", usecols=lambda column: column != 'Unnamed: 0')
# stock = pd.read_csv("data\sensor_stock_levels.csv", usecols=lambda column: column != 'Unnamed: 0')


def convert_timestamp_to_hourly(data: pd.DataFrame = None, column: str = 'timestamp'):
    """
    Rounds up the timestamp information to the nearest hour. 

    Args:
        data (pd.DataFrame, optional): DataFrame containing the timestamp column. Defaults to None.
        column (str, optional): The column containing timestamp data. Defaults to None.

    Returns:
        _type_: pd.DataFrame. New DataFrame with the hours rounded off in the timestamp column.
    """
    data[column] = pd.to_datetime(data[column])
    dummy = data.copy()
    new_ts = dummy[column].tolist()
    new_ts = [i.strftime('%Y-%m-%d %H:00:00') for i in new_ts]
    new_ts = [datetime.strptime(i, '%Y-%m-%d %H:00:00') for i in new_ts]
    dummy[column] = new_ts
    return dummy



def perform_groupby(data: str, groupby_columns: str, aggregate_dict: dict) -> pd.DataFrame:
    """
    Perform groupby operation on a pandas DataFrame
            
    Args:
        data (str): The Pandas DataFrame to perform groupby on.
        groupby_columns (str): A list of columns to groupby.
        aggregate_dict (dict): A dictionary mapping columns to aggregation functions.

    Returns:
        A new DataFrame with the groupby operation applied
    """
    grouped_data= data.groupby(groupby_columns).agg(aggregate_dict).reset_index()
    return grouped_data
    
    

def process():
    """function to simplify the ETL process.

    Returns:
        pd.DataFrame: returns a cleaned DataFrame
    """
    path = "data/sales.csv"
    file_name = path.split('/')[1].split('.')[0]
    df = read_data(path)
    results = convert_timestamp_to_hourly(df)
    results = perform_groupby(results,["timestamp", "product_id"], {'quantity': 'sum'} )
    results.to_csv(f"data/{file_name}_agg.csv", index=False) #sales_agg.csv
    return None

