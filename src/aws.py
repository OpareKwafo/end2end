from pathlib import Path
from dotenv import load_dotenv
import boto3
import os
from typing import Tuple
import pandas as pd
from io import StringIO
import gspread
from googleapiclient.discovery import build
#from oauth2client.service_account import ServiceAccountCredentials
from google.oauth2.service_account import Credentials


def aws_authentication() -> Tuple[boto3.client, str]:
    
    load_dotenv(".env")
    client = boto3.client('s3', aws_access_key_id=os.getenv('ACCESS_KEY_ID'), 
                          aws_secret_access_key=os.getenv('SECRET_ACCESS_KEY'), 
                          region_name=os.getenv('REGION'))

    bucket_name = os.getenv("BUCKET_NAME")
    return client, bucket_name


def upload_to_s3(df: pd.Dataframe, filename: str) -> bool:
    
    #first authenticate with AWS
    client, bucket_name = aws_authentication()
    
    # upload to S3
    try:
        csv_buffer = StringIO()
        response = client.put_object(
            ACL = 'private',
            Body = csv_buffer.get_value(),
            Bucket = bucket_name,
            Key = filename + '.csv'
        )
    except Exception as e:
        raise Exception(f"Could NOT upload file to S3: {e}")
    return True
    
    
def read_from_s3(file_name: str) -> pd.DataFrame:
    
    # first authenticate with AWS
    client, bucket_name = aws_authentication()
    
    # Read file
    try:
        response = client.get_object(Bucket=bucket_name, Key=file_name)  #response is a JSON
        df = pd.read_csv(response["Body"])  
    except Exception as e:
        raise Exception(f"There was an error reading {file_name} from s3")
    
    # return the dataframe
    return df
    

def upload_to_google_sheet(spreadsheet_id:str, df: pd.DataFrame, worksheet_name:str) -> bool:
    
    #authenticate
    SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
    
    load_dotenv(Path('.env'))
    
    type = os.getenv("TYPE")
    auth_uri = os.getenv("AUTH_URI")
    token_uri = os.getenv("TOKEN_URI")
    auth_provider_x509_cert_url = os.getenv("AUTH_PROVIDER_X509_CERT_URL")
    project_id = os.getenv("PROJECT_ID")
    private_key_id = os.getenv("PRIVATE_KEY_ID")
    private_key = os.getenv("PRIVATE_KEY").replace("\\n", "\n")
    client_email = os.getenv("CLIENT_EMAIL")
    client_id = os.getenv("CLIENT_ID")
    client_x509_cert_url = os.getenv("CLIENT_X509_CERT_URL")
    
    credentials = {
        
        "type": type,
        "project_id": project_id,
        "private_key_id": private_key_id,
        "private_key": private_key,
        "client_email": client_email,
        "client_id": client_id,
        "auth_uri": auth_uri,
        "token_uri": token_uri,
        "auth_provider_x509_cert_url": auth_provider_x509_cert_url,
        "client_x509_cert_url": client_x509_cert_url,
    }
    
    #authorize
    gc = gspread.service_account_from_dict(credentials)

    #opening the worksheet
    try:
        worksheet = gc.open_by_key(spreadsheet_id).worksheet(worksheet_name)
    except gspread.exceptions.WorksheetNotFound:
        worksheet = gc.open_by_key(spreadsheet_id).add_worksheet(worksheet_name, 1, 1)
        
    #clear existing content in worksheet
    worksheet.clear()

    #convert timestamp column to string
    df = df.astype(str)

    #write df to worksheet
    cell_list = worksheet.update([df.columns.values.tolist()] + df.values.tolist())
    if cell_list:
        return True
    else:
        return False