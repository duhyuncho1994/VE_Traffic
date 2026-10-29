
from datetime import timedelta
import socket

import pendulum
from airflow.sdk import dag, task


@dag(
    dag_id="spark_failure_test",
    start_date=pendulum.datetime(
        2026, 10, 28, tz="Europe/Berlin"
    ),
    schedule=None,
    catchup=False,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(seconds=30),
    },
    tags=["spark", "failure-test"],
)
def spark_failure_test():

    @task
    def test_spark_connection():
        host = "traffic-spark"
        port = 7078  # 의도적으로 잘못된 포트

        print(f"Testing Spark connection: {host}:{port}")

        try:
            with socket.create_connection(
                (host, port),
                timeout=5
            ):
                print("Spark connection successful")

        except OSError as error:
            raise RuntimeError(
                f"Spark connection failed: {error}"
            ) from error

    test_spark_connection()


spark_failure_test()
