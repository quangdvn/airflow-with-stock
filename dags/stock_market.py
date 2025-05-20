from datetime import datetime

import requests
from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sensors.base import PokeReturnValue

from include.stock.tasks import (BUCKET_NAME, _get_formatted_csv,
                                 _get_stock_prices, _store_prices)

SYMBOL = 'NVDA'


# Define the basic parameters of the DAG, like schedule and start_date
@dag(
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args={"owner": "quangdvn", "retries": 3},
    tags=["stock_market"],
)
def stock_market():
  # Define that this task updates the `current_astronauts` Dataset
  @task.sensor(poke_interval=30, timeout=300, mode="poke")
  def is_api_available() -> PokeReturnValue:
    """
    This task uses the requests library to retrieve a list of Astronauts
    currently in space. The results are pushed to XCom with a specific key
    so they can be used in a downstream pipeline. The task returns a list
    of Astronauts to be used in the next task.
    """
    api = BaseHook.get_connection("stock_api")
    url = f"{api.host}{api.extra_dejson['endpoint']}"
    print("ABC", url)
    response = requests.get(url, headers=api.extra_dejson['headers'])
    print("123", response)
    done_condition = response.json()['finance']['result'] is None
    return PokeReturnValue(is_done=done_condition, xcom_value=url)

  get_stock_prices = PythonOperator(
      task_id="get_stock_prices",
      python_callable=_get_stock_prices,
      op_kwargs={"path": "{{ti.xcom_pull(task_ids='is_api_available')}}", "symbol": SYMBOL}
  )

  store_prices = PythonOperator(
      task_id="store_prices",
      python_callable=_store_prices,
      op_kwargs={"stock": "{{ti.xcom_pull(task_ids='get_stock_prices')}}"}
  )

  format_prices = DockerOperator(
      task_id="format_prices",
      image="airflow-with-stock/stock-app",
      container_name="format_prices",
      api_version='auto',
      auto_remove='success',
      docker_url='tcp://docker-proxy:2375',
      network_mode='container:spark-master',
      tty=True,
      xcom_all=False,
      mount_tmp_dir=False,
      environment={
          'SPARK_APPLICATION_ARGS': "{{ti.xcom_pull(task_ids='store_prices')}}"
      }
  )

  get_formatted_csv = PythonOperator(
      task_id="get_formatted_csv",
      python_callable=_get_formatted_csv,
      op_kwargs={
          'path': "{{ti.xcom_pull(task_ids='store_prices')}}"
      }
  )

  @task()
  def load_to_postgres(object_path: str):
    from io import BytesIO

    import pandas as pd
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    from include.stock.tasks import _get_minio_client
    client = _get_minio_client()
    response = client.get_object(bucket_name=BUCKET_NAME, object_name=object_path)
    df = pd.read_csv(BytesIO(response.read()))
    df['timestamp'] = df['timestamp'].astype(int)

    hook = PostgresHook(postgres_conn_id="postgres")
    engine = hook.get_sqlalchemy_engine()
    df.to_sql("stock_market", engine, if_exists="append", index=False)
    return f"{len(df)} records inserted."

  api_check = is_api_available()
  load_result = load_to_postgres(get_formatted_csv.output)

  api_check >> get_stock_prices >> store_prices >> format_prices >> get_formatted_csv >> load_result  # type: ignore


# Instantiate the DAG
stock_market()
