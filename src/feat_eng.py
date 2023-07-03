import pandas as pd


def merge_dataframe(dataframe_to_keep: pd.DataFrame, dataframe_to_add: pd.DataFrame, merging_columns: str, merging_method: str) -> pd.DataFrame:
    """
    Merges 2 or more DataFrames.

    Args:
        dataframe_to_keep (pd.DataFrame): DataFrame you want to keep all information.
        dataframe_to_add (pd.DataFrame): DataFrame we want to add.
        merging_columns (pd.DataFrame): common columns to merge on.
        merging_method (pd.DataFrame): decides how to perform the merge.

    Returns:
        pd.DataFrame: DataFrame with the new DataFrame added.
    """
    results = dataframe_to_keep.merge(dataframe_to_add, on=merging_columns, how=merging_method)
    return results
    
    
def fill_null_with_zeros(dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Fills the null values in a DataFrame with zeros.

    Args:
        dataframe (pd.DataFrame): Pandas DataFrame containing missing or null values.

    Returns:
        pd.DataFrame: New Pandas DataFrame with null values filled with zeros.
    """
    return dataframe.fillna(0)

def create_dataframe(dataframe: pd.DataFrame, columns_to_extract: str) -> pd.DataFrame:
    """
    This function creates a new DataFrame from an existing DataFrame.

    Args:
        dataframe (pd.DataFrame): The existing Pandas DataFrame containing our interested features.
        columns_to_extract (str): The columns we need in our new DataFrame

    Returns:
        pd.DataFrame: New DataFrame with our interested features.
    """
    new_dataframe = dataframe[columns_to_extract]
    new_dataframe = new_dataframe.drop_duplicates()
    return new_dataframe


def extract_datetime_features(dataframe: pd.DataFrame, column_name: str) -> pd.DataFrame:
    """
    This function extracts features from a datetime column and drops the datetime column after.

    Args:
        dataframe (pd.DataFrame): DataFrame containing our datetime information.
        column_name (str): Datetime column.

    Returns:
        pd.DataFrame: DataFrame with new datetime features.
    """
  
    if dataframe[column_name].dtype == 'datetime64[ns]':
        dataframe['Day'] = dataframe[column_name].dt.dayofweek
        dataframe['Hour'] = dataframe[column_name].dt.hour
        dataframe.drop(column_name)
    else:
        dataframe[column_name] = pd.to_datetime(dataframe[column_name])
        dataframe['Day'] = dataframe[column_name].dt.dayofweek
        dataframe['Hour'] = dataframe[column_name].dt.hour
        dataframe.drop(column_name)
    return dataframe
      
     
def categorical_to_numeric(dataframe: pd.DataFrame, categorical_column: str) -> pd.DataFrame:
    """
    Converts a categorical column to numeric using one-hot encoding and drops categorical column after.

    Args:
        dataframe (pd.DataFrame): DataFrame containing our categorical column.
        categorical_column (str): name of categorical column.

    Returns:
        pd.DataFrame: DataFrame with the one-hot encoding features.
    """
    encoded_dataframe = pd.get_dummies(dataframe[categorical_column])
    dataframe = dataframe.drop(categorical_column)
    return encoded_dataframe


def combine_dataframes(list_of_dataframes: list) -> pd.DataFrame:
    """
    The function takes two or more DataFrames of the same size and combines them.

    Args:
        list_of_dataframes (list): A list of DataFrames.

    Returns:
        pd.DataFrame: DataFrame with all listed DataFrames combined.
    """
    combined_dataframe = pd.concat([list_of_dataframes], axis=1)
    return combined_dataframe
    
    
def process():
    df = merge_dataframe()
    result = fill_null_with_zeros(df)
    result = create_dataframe(result)
    result = extract_datetime_features(result)
    result = categorical_to_numeric()
    result = combine_dataframes()
    return result
    