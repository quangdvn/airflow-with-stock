from airflow import XComArg
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.sdk.definitions.asset import Asset
from pendulum import datetime, duration

COCKTAIL = Asset(uri="/tmp/cocktail_data.json")


@dag(
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args={"owner": "quangdvn", "retries": 3},
    tags=["extractor"],
)
def extractor():
  @task(outlets=[COCKTAIL], retry_exponential_backoff=True, retry_delay=duration(hours=1))
  def get_cocktail_data():
    import requests

    print("=============")
    api_url = "https://www.thecocktaildb.com/api/json/v1/1/random.php"
    response = requests.get(api_url)
    # print('123', response.content)
    with open(COCKTAIL.uri, "wb") as f:
      f.write(response.content)
    print("=============")
    return len(response.content)

  @task()
  def check_size(size: XComArg):
    print(f"The size of the cocktail data is: {size} bytes")

  check_size(get_cocktail_data())


@dag(
    start_date=datetime(2025, 1, 1),
    schedule=[COCKTAIL],
    catchup=True,
    default_args={"owner": "quangdvn", "retries": 1},
    tags=["ecom"],
    dagrun_timeout=duration(minutes=10),
    max_consecutive_failed_dag_runs=2
)
def ecom_dag():
  ta = EmptyOperator(task_id="ta")


extractor()
ecom_dag()
