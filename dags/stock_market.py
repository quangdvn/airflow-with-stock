from datetime import datetime
from typing import Any

import requests
from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.providers.discord.notifications.discord import DiscordNotifier
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
    on_success_callback=DiscordNotifier(
        discord_conn_id="discord",
        text="Stock market DAG completed successfully",
    ),
    on_failure_callback=DiscordNotifier(
        discord_conn_id="discord",
        text="Stock market DAG failed",
    ),
)
def stock_market():
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

  @task
  def get_stock_prices(path: Any) -> str:
    return _get_stock_prices(path=path, symbol=SYMBOL)

  @task
  def store_prices(stock: Any) -> str:
    return _store_prices(stock=stock)

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

  @task
  def get_formatted_csv(path: Any) -> str:
    return _get_formatted_csv(path=path)

  @task
  def load_to_postgres(object_path: Any) -> str:
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
    df.to_sql("stock_market", engine, if_exists="replace", index=False)
    return f"{len(df)} records inserted."

  # Define task dependencies using both functional style and bitshift operators
  api_check = is_api_available()
  stock_prices = get_stock_prices(api_check)
  stored_prices = store_prices(stock_prices)

  # Connect DockerOperator to the task flow
  stored_prices >> format_prices
  formatted_csv = get_formatted_csv(stored_prices)
  format_prices >> formatted_csv
  load_result = load_to_postgres(formatted_csv)


# Instantiate the DAG
stock_market()
