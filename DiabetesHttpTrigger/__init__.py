import logging
import os
import requests
import pandas as pd

import azure.functions as func

def score_model(dataset: pd.DataFrame):

  url = 'https://adb-5873545433774891.11.azuredatabricks.net/model/diabetes-random-forest/1/invocations'
  headers = {'Authorization': f'Bearer {os.environ.get("DATABRICKS_TOKEN")}'}
  data_json = dataset.to_dict(orient='split')
  response = requests.request(method='POST', headers=headers, url=url, json=data_json)

  if response.status_code != 200:
    raise Exception(f'Request failed with status {response.status_code}, {response.text}')
  return response.json()


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    name = req.params.get('name')

    df = pd.read_csv("dataset\diabetes-test.csv", sep=';')

    logging.info(df.head())

    result = score_model(df)

    if result:
        return func.HttpResponse(f"Risultato del modello: {result}. This HTTP triggered function executed successfully.")
    else:
        return func.HttpResponse(
             "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.",
             status_code=200
        )
