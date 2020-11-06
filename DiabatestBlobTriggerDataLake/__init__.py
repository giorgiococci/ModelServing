import logging
import os
import requests
import pandas as pd
from io import StringIO 

# import uuid, sys
import azure.functions as func

from azure.identity import ClientSecretCredential
from azure.storage.filedatalake import DataLakeServiceClient

import datetime

def initialize_adls(storage_account, client_id, client_secret, tenant_id):
    """Initialize the connection to an Azure Data Lake Gen 2

    Args:
        storage_account (string): The storage account name
        client_id (string): The service principal client id
        client_secret (string): The service principal client secret
        tenant_id (string): The azure tenant id

    Returns:
        [DataLakeServiceClient]: An Azure Data Lake Gen 2 client
    """
    try:  

        credential = ClientSecretCredential(tenant_id, client_id, client_secret)

        service_client = DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format(
            "https", storage_account), credential=credential)
    
        return service_client

    except Exception as e:
        print(e)


def upload_file_to_directory(service_client, file_system, directory, source_content, target_file_name):
    """Upload a content string into a blob in Azure Data Lake Gen 2

    Args:
        service_client ([DataLakeServiceClient]): The ADLS client. 
        file_system (string): The target container name. 
        directory (string): Path + name of the target directory. 
        source_content ([string]): The content you would save on ADLS blob file. 
        target_file_name (string): Name of the the file. 
    """
    try:

        file_system_client = service_client.get_file_system_client(file_system=file_system)

        directory_client = file_system_client.get_directory_client(directory)
        
        file_client = directory_client.create_file(target_file_name)
        file_client.append_data(data=source_content, offset=0, length=len(source_content))

        file_client.flush_data(len(source_content))

    except Exception as e:
      print(e) 


def score_model(dataset: pd.DataFrame):
    url = os.environ.get("MODEL_URL")
    headers = {'Authorization': f'Bearer {os.environ.get("DATABRICKS_TOKEN")}'}
    data_json = dataset.to_dict(orient='split')
    response = requests.request(method='POST', headers=headers, url=url, json=data_json)

    if response.status_code != 200:
        raise Exception(f'Request failed with status {response.status_code}, {response.text}')
    return response.json()


def main(myblob: func.InputStream):
    logging.info(f"Python blob trigger function processed blob \n"
                 f"Name: {myblob.name}\n"
                 f"Blob Size: {myblob.length} bytes")

    text = myblob.read().decode('utf-8')
    df = pd.read_csv(StringIO(text),  sep=';')

    prediction = score_model(df)
    df['prediction'] = prediction

    json_result = df.to_json(orient="records")

    logging.info(json_result)

    storage_account = os.environ.get("ADLS_STORAGE_ACCOUNT")
    client_id = os.environ.get("ADLS_CLIENT_ID")
    client_secret = os.environ.get("ADLS_CLIENT_SECRET")
    tenant_id = os.environ.get("TENANT_ID")

    utc_timestamp = datetime.datetime.utcnow().strftime("%Y%m%d%H%M%S")

    service_client = initialize_adls(storage_account, client_id, client_secret, tenant_id)
    upload_file_to_directory(service_client, "diabetes", "diabetes-prediction", json_result, f"prediction_{utc_timestamp}.json")
