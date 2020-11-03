import logging
import os
import requests
import pandas as pd

import azure.functions as func

def score_model(dataset: pd.DataFrame):

  url = os.environ.get("MODEL_URL")
  headers = {'Authorization': f'Bearer {os.environ.get("DATABRICKS_TOKEN")}'}
  data_json = dataset.to_dict(orient='split')
  response = requests.request(method='POST', headers=headers, url=url, json=data_json)

  if response.status_code != 200:
    raise Exception(f'Request failed with status {response.status_code}, {response.text}')
  return response.json()


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    status_code = 200

    try:
        df = pd.read_csv("dataset\diabetes-test.csv", sep=';')

        logging.debug(df.head())

        prediction = score_model(df)
        df['prediction'] = prediction

        json_result = df.to_json(orient="records")
        return func.HttpResponse(json_result, status_code = status_code)

    except:
        status_code = 400
        return func.HttpResponse("Error", status_code = status_code)

