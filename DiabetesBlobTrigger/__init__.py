import logging
import os
import requests
import pandas as pd
from io import StringIO 

from datetime import datetime, timedelta

import azure.functions as func


def score_model(dataset: pd.DataFrame):
  """[summary]

  Args:
      dataset (pd.DataFrame): [description]

  Raises:
      Exception: [description]

  Returns:
      [type]: [description]
  """
  url = os.environ.get("MODEL_URL")
  headers = {'Authorization': f'Bearer {os.environ.get("DATABRICKS_TOKEN")}'}
  data_json = dataset.to_dict(orient='split')
  response = requests.request(method='POST', headers=headers, url=url, json=data_json)

  if response.status_code != 200:
    raise Exception(f'Request failed with status {response.status_code}, {response.text}')
  return response.json()


def main(inputblob: func.InputStream, outputblob: func.Out[bytes], context: func.Context):
    logging.info(f"Python blob trigger function processed blob \n"
                 f"Name: {inputblob.name}\n"
                 f"Blob Size: {inputblob.length} bytes")

    text = inputblob.read().decode('utf-8')
    df = pd.read_csv(StringIO(text),  sep=';')

    prediction = score_model(df)
    df['prediction'] = prediction

    json_result = df.to_json(orient="records")

    logging.info(json_result)

    outputblob.set(json_result)