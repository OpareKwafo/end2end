import pandas as pd
    
def create_dataframe(sales_df: pd.DataFrame, columns_to_extract: list[str]) -> pd.DataFrame:
    """
    This function creates a new DataFrame from an existing DataFrame.

    Args:
        dataframe (pd.DataFrame): The existing sales DataFrame containing our interested features.
        columns_to_extract (str): The columns we need in our new DataFrame

    Returns:
        pd.DataFrame: New DataFrame with our interested features.
    """
    new_dataframe = sales_df[columns_to_extract]
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
    if dataframe[column_name].dtype != 'datetime64[ns]':
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
    dataframe = pd.get_dummies(dataframe, columns=[categorical_column])
    dataframe = dataframe.drop(categorical_column)
    return dataframe
    
    
def process():
    #read dataframes
    sales_df = pd.read_csv("data\sales_agg.csv")
    temp_df = pd.read_csv("data\sensor_storage_temperature_agg.csv")
    stock_df = pd.read_csv("data\sensor_stock_levels_agg.csv")
    
    #merge all dataframes
    merged_df = stock_df.merge(sales_df, on=['timestamp', 'product_id'], how='left')
    merged_df = merged_df.merge(temp_df, on='timestamp', how='left')
    
    #fill missing values
    merged_df = merged_df.fillna(0)
    
    #add additional features from sales table
    categories_df = create_dataframe(sales_df, ['product_id', 'category'] )
    categories_df.drop_duplicates()
    unitPrice_df = create_dataframe(sales_df, ['product_id', 'unit_price'] )
    unitPrice_df.drop_duplicates()
    
    #merge extracted columns with the main dataframe above
    merged_df = merged_df.merge(categories_df, on='product_id', how='left')
    merged_df = merged_df.merge(unitPrice_df, on='product_id', how='left')
    
    #extract day and hour from the merged dataframe
    merged_df = extract_datetime_features(merged_df)
    merged_df = categorical_to_numeric(merged_df, "category")
    
    #drop the product id column. no need for machine learning
    merged_df = merged_df.drop("product_id")
    merged_df.to_csv()
    
    return None
    