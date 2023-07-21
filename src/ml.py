import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error
from src.utils import read_data
from src.aws import read_from_s3
from aws import upload_to_google_sheet


def create_model(X, y):
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=101)
    rf_model = RandomForestRegressor(n_estimators=100, random_state=42)
    rf_model.fit(X_train, y_train)
    predictions = rf_model.predict(X_test)
    return predictions, y_test


def get_metric(predictions, y_test):
    mae = mean_absolute_error(y_test=y_test, predictions=predictions)
    mse = mean_squared_error(y_test, predictions)
    return mae, mse


def process():
    #read data and select X and y
    df = read_from_s3("features.csv")
    X = df.drop('estimated_stock_pct', axis=1)
    y = df['estimated_stock_pct']
    
    #create model and predictions
    predictions, y_test = create_model(X, y)
    mae, mse = get_metric(predictions, y_test)
    spreadsheet_id = "1UnLoWDS8Y4byNNwfVNI0tOK_CLXY_r7Cf_pj4IZW8-s"
    upload_to_google_sheet(spreadsheet_id=spreadsheet_id, df=df, worksheet_name="features")
    print("File sucessfully uploaded to google sheets")
    
    
    
    