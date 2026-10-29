
from datetime import timedelta
from pathlib import Path
import csv
import os
import subprocess

import paramiko
import pendulum
import psycopg

from airflow.sdk import dag, task
from batch_logging import on_task_success, on_task_failure


DB_CONFIG = {
    "host": "postgres",
    "port": 5432,
    "dbname": "traffic_db",
    "user": "traffic_user",
    "password": os.environ["TRAFFIC_DB_PASSWORD"],
}

REPORT_DIR = Path("/opt/airflow/data/reports")
REPORT_FILE = REPORT_DIR / "traffic_accuracy_latest.csv"


@dag(
    dag_id="traffic_batch_pipeline",
    start_date=pendulum.datetime(
        2026, 10, 27, tz="Europe/Berlin"
    ),
    schedule="0 9 * * *",
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "on_success_callback": on_task_success,
        "on_failure_callback": on_task_failure,
    },
    tags=["traffic", "batch", "incremental", "spark"],
)
def traffic_batch_pipeline():

    @task
    def load_traffic_csv():
        env = os.environ.copy()
        env.update({
            "TRAFFIC_DATA_DIR": "/opt/airflow/data",
            "TRAFFIC_DB_HOST": "postgres",
            "TRAFFIC_DB_PORT": "5432",
        })

        subprocess.run(
            [
                "python",
                "/opt/airflow/scripts/incremental_load.py",
            ],
            env=env,
            check=True,
        )

        print("Incremental ETL completed successfully")

    @task
    def check_data_quality():
        with psycopg.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT
                        COUNT(*) AS total_rows,
                        COUNT(*) FILTER (
                            WHERE source_file IS NULL
                               OR source_type IS NULL
                               OR datum IS NULL
                        ) AS null_rows,
                        COUNT(DISTINCT source_type)
                            AS source_types
                    FROM traffic_raw
                """)

                total_rows, null_rows, source_types = (
                    cur.fetchone()
                )

                cur.execute("""
                    SELECT COUNT(*)
                    FROM (
                        SELECT
                            source_file,
                            source_type,
                            record_id,
                            strom,
                            kfz_klasse,
                            mstimestamp,
                            stimestamp,
                            datum,
                            geschwindigkeit_1,
                            geschwindigkeit_2,
                            q_wert
                        FROM traffic_raw
                        GROUP BY
                            source_file,
                            source_type,
                            record_id,
                            strom,
                            kfz_klasse,
                            mstimestamp,
                            stimestamp,
                            datum,
                            geschwindigkeit_1,
                            geschwindigkeit_2,
                            q_wert
                        HAVING COUNT(*) > 1
                    ) duplicates
                """)

                duplicate_groups = cur.fetchone()[0]

        print(f"Total rows: {total_rows}")
        print(f"Null rows: {null_rows}")
        print(f"Duplicate groups: {duplicate_groups}")
        print(f"Source types: {source_types}")

        if total_rows == 0:
            raise ValueError("No traffic data loaded")

        if null_rows > 0:
            raise ValueError("Null values detected")

        if duplicate_groups > 0:
            raise ValueError(
                "Exact duplicate records detected"
            )

        if source_types != 2:
            raise ValueError(
                "Missing KONTROLLE or TRACKING data"
            )

        print("Data quality check passed")

    @task
    def spark_aggregation():
        print("Connecting to Spark via SSH")

        client = paramiko.SSHClient()
        client.load_host_keys(
            "/home/airflow/.ssh/known_hosts"
        )
        client.set_missing_host_key_policy(
            paramiko.RejectPolicy()
        )

        try:
            client.connect(
                hostname="traffic-spark",
                username="spark",
                key_filename=(
                    "/home/airflow/.ssh/id_ed25519"
                ),
                timeout=10,
                look_for_keys=False,
                allow_agent=False,
            )

            command = (
                ". /opt/traffic/spark/spark-env.sh && "
    		"/opt/spark/bin/spark-submit "
    		"--master spark://traffic-spark:7077 "
    		"--deploy-mode client "
    		"--total-executor-cores 2 "
    		"--executor-memory 1g "
    		"/opt/traffic/spark/write_summary.py"
            )

            _, stdout, stderr = client.exec_command(
                command,
                timeout=300,
            )

            output = stdout.read().decode()
            errors = stderr.read().decode()

            print(output)

            if errors:
                print(errors[-3000:])

            exit_code = stdout.channel.recv_exit_status()

            print(f"Spark exit code: {exit_code}")

            if exit_code != 0:
                raise RuntimeError(
                    f"Spark aggregation failed: {exit_code}"
                )

            if "PIPELINE SUCCESS" not in output:
                raise RuntimeError(
                    "Spark success marker not found"
                )

            print("Spark aggregation completed")

        finally:
            client.close()

    @task
    def analyze_accuracy():
        with psycopg.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT
                        kp,
                        datum,
                        manual_count,
                        ai_count,
                        difference,
                        error_pct
                    FROM vw_traffic_accuracy
                    ORDER BY error_pct DESC
                """)

                results = cur.fetchall()

        if not results:
            raise ValueError(
                "No accuracy analysis results found"
            )

        warning_count = 0
        report_rows = []

        for kp, datum, manual, ai, diff, error in results:
            status = "OK"

            if error is not None and error > 5:
                status = "WARNING"
                warning_count += 1

            print(
                f"[{status}] KP={kp}, Date={datum}, "
                f"Manual={manual}, AI={ai}, "
                f"Difference={diff}, Error={error}%"
            )

            report_rows.append([
                kp,
                datum,
                manual,
                ai,
                diff,
                error,
                status,
            ])

        REPORT_DIR.mkdir(
            parents=True,
            exist_ok=True,
        )

        temp_file = REPORT_DIR / (
            "traffic_accuracy_latest.tmp"
        )

        with temp_file.open(
            "w",
            newline="",
            encoding="utf-8-sig",
        ) as file:
            writer = csv.writer(file)

            writer.writerow([
                "kp",
                "datum",
                "manual_count",
                "ai_count",
                "difference",
                "error_pct",
                "status",
            ])

            writer.writerows(report_rows)

        temp_file.replace(REPORT_FILE)

        print(f"Total comparisons: {len(results)}")
        print(f"Warnings (>5%): {warning_count}")
        print(f"CSV report saved: {REPORT_FILE}")
        print("Accuracy analysis completed")

    load_task = load_traffic_csv()
    quality_task = check_data_quality()
    spark_task = spark_aggregation()
    accuracy_task = analyze_accuracy()

    load_task >> quality_task >> spark_task >> accuracy_task


traffic_batch_pipeline()

