
import paramiko

import pendulum
from airflow.sdk import dag, task


@dag(
    dag_id="spark_integration_test",
    start_date=pendulum.datetime(
        2026, 10, 28, tz="Europe/Berlin"
    ),
    schedule=None,
    catchup=False,
    tags=["spark", "ssh", "testing"],
)
def spark_integration_test():

    @task
    def run_spark_test():
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(
            paramiko.AutoAddPolicy()
        )

        try:
            client.connect(
                hostname="traffic-spark",
                username="spark",
                key_filename="/home/airflow/.ssh/id_ed25519",
                timeout=10,
                look_for_keys=False,
                allow_agent=False,
            )

            command = (
                ". /home/spark/spark-env.sh && "
                "/opt/spark/bin/spark-submit "
                "--master spark://traffic-spark:7077 "
                "--deploy-mode client "
                "--total-executor-cores 2 "
                "--executor-memory 1g "
                "/opt/traffic/spark/cluster_test.py"
            )

            _, stdout, stderr = client.exec_command(
                command, timeout=180
            )

            output = stdout.read().decode()
            errors = stderr.read().decode()

            print(output)
            print(errors)

            exit_code = stdout.channel.recv_exit_status()

            if exit_code != 0:
                raise RuntimeError(
                    f"Spark job failed: exit code {exit_code}"
                )

            if "CLUSTER TEST SUCCESS" not in output:
                raise RuntimeError(
                    "Spark success marker not found"
                )

            print("AIRFLOW_SPARK_INTEGRATION_SUCCESS")

        finally:
            client.close()

    run_spark_test()


spark_integration_test()
