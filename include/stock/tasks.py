

import json

from airflow.exceptions import AirflowNotFoundException
from airflow.hooks.base import BaseHook
from minio import Minio

BUCKET_NAME = 'stock-market'


def _get_minio_client():
  minio = BaseHook.get_connection("minio")
  client = Minio(
      endpoint=minio.extra_dejson['endpoint_url'].split('//')[1],
      access_key=minio.extra_dejson['aws_access_key_id'],
      secret_key=minio.extra_dejson['aws_secret_access_key'],
      secure=False
  )
  return client


def _get_stock_prices(path, symbol):
  import requests

  url = f"{path}{symbol}?metrics=high&interval=1d&range=2y"
  api = BaseHook.get_connection("stock_api")
  response = requests.get(url=url, headers=api.extra_dejson['headers'])
  # print("===============")
  # print(response.json())
  # print("===============")
  return json.dumps(response.json()['chart']['result'][0])


def _store_prices(stock):
  from datetime import datetime
  from io import BytesIO

  date_str = datetime.now().strftime('%Y%m%d')
  client = _get_minio_client()

  if not client.bucket_exists(BUCKET_NAME):
    client.make_bucket(BUCKET_NAME)
  stock = json.loads(stock)
  symbol = stock['meta']['symbol']
  data = json.dumps(stock, ensure_ascii=False).encode('utf-8')
  objw = client.put_object(
      bucket_name=BUCKET_NAME,
      object_name=f"{symbol}/{date_str}/prices.json",
      data=BytesIO(data),
      length=len(data),
      content_type='application/json'
  )
  return f'{objw.bucket_name}/{symbol}/{date_str}'


def _get_formatted_csv(path):
  client = _get_minio_client()
  prefix_name = f"{'/'.join(path.split('/')[1:])}/formatted_prices"
  print('321', prefix_name)
  objects = client.list_objects(BUCKET_NAME, prefix=prefix_name, recursive=True)
  print("==============")
  print('123', objects)
  print("==============")
  for obj in objects:
    print('456', obj.object_name)
    if obj.object_name and obj.object_name.endswith('.csv'):
      return obj.object_name
  raise AirflowNotFoundException('The CSV file does not exist')
