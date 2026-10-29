
from datetime import timedelta

import pendulum
from airflow.sdk import dag, task


@dag(
    dag_id="traffic_retry_test",
    start_date=pendulum.datetime(
        2026, 10, 27, tz="Europe/Berlin"
    ),
    schedule=None,
    catchup=False,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(seconds=30),
    },
    tags=["traffic", "testing", "retry"],
)
def traffic_retry_test():

    @task
    def simulate_failure():
        print("Simulating a pipeline failure")
        raise RuntimeError("Test failure: retry verification")

    simulate_failure()


traffic_retry_test()
