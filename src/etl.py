import pandas as pd
import numpy as np
from datetime import datetime

# sales = pd.read_csv("data\sales.csv", usecols=lambda column: column != 'Unnamed: 0')
# temp = pd.read_csv("data\sensor_storage_temperature.csv", usecols=lambda column: column != 'Unnamed: 0')
# stock = pd.read_csv("data\sensor_stock_levels.csv", usecols=lambda column: column != 'Unnamed: 0')

def read_data(file_path: str) -> pd.DataFrame:
    """
    Reads data from a CSV file.
    Args:
        file_path (str): the file path.

    Returns:
        the data with the unnamed:0 column dropped.
    """
    df = pd.read_csv(file_path: str) 
    df.drop("Unnamed: 0", axis=1, inplace=True, errors='ignore')
    return df


def convert_data_types(data: str) -> pd.DataFrame:
    """
    Converts the dataframe columns to the appropriate types.

    Args:
        data (str): The pandas Dataframe containing the columns.

    Returns:
        pd.DataFrame: New DataFrame with the right data types.
    """
    for column in data.columns:
        current_type = data[column].dtype
        #convert object columns to datetime if possible
        if current_type == 'object':
            try:
                data[column] = pd.to_datetime(data[column])
                continue
            except ValueError:
                pass
            
        #convert numeric strings to numeric types
        if current_type == 'object':
            try:
                data[column] = pd.to_numeric(data[column])
                continue
            except ValueError:
                pass
            
    return data



def convert_timestamp_to_hourly(data: pd.DataFrame = None, column: str = None):
    """
    Rounds up the timestamp information to the nearest hour. 

    Args:
        data (pd.DataFrame, optional): DataFrame containing the timestamp column. Defaults to None.
        column (str, optional): The column containing timestamp data. Defaults to None.

    Returns:
        _type_: pd.DataFrame. New DataFrame with the hours rounded off in the timestamp column.
    """
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
    grouped_data= data.groupby(groupby_columns).agg(aggregate_dict).reset_index
    return grouped_data
    
    

def process():
    """function to simplify the ETL process.

    Returns:
        pd.DataFrame: returns a cleaned DataFrame
    """
    df = read_data()
    results = convert_data_types(df)
    results = convert_timestamp_to_hourly(results)
    results = perform_groupby(results)
    return results

