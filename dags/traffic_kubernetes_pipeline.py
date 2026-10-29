import logging
import time
import uuid
from datetime import datetime, timezone

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from kubernetes import client

from traffic_k8s_client import get_k8s_client


NAMESPACE = "spark"
SPARK_IMAGE = "traffic-spark-k8s:1.1"
TIMEOUT_SECONDS = 900
POLL_SECONDS = 5

log = logging.getLogger(__name__)



def log_failed_pod(api, pod_name):
    """Log recent Kubernetes Pod output for troubleshooting."""
    if not pod_name:
        return

    try:
        pod_logs = api.read_namespaced_pod_log(
            name=pod_name,
            namespace=NAMESPACE,
            tail_lines=100,
            timestamps=True,
        )

        # Avoid printing full Kubernetes Pod specifications or
        # environment variables that may contain credentials.
        log.error(
            "Kubernetes Pod failure logs [%s]:\n%s",
            pod_name,
            pod_logs,
        )

    except Exception as exc:
        log.warning(
            "Could not retrieve logs for Pod %s: %s",
            pod_name,
            type(exc).__name__,
        )


def get_spark_execution_record(airflow_run_id):
    """Read the existing Spark execution record without modifying it."""
    import os
    import psycopg2

    connection = psycopg2.connect(
        host="postgres",
        port=5432,
        dbname="traffic_db",
        user=os.environ["TRAFFIC_DB_USER"],
        password=os.environ["TRAFFIC_DB_PASSWORD"],
        connect_timeout=10,
    )

    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT spark_app_name, submitter_name, status
                FROM public.spark_job_execution_log
                WHERE airflow_run_id = %s
                """,
                (airflow_run_id,),
            )
            return cursor.fetchone()
    finally:
        connection.close()


def register_spark_execution(airflow_run_id, app_name, submitter_name):
    """Register one Spark execution without overwriting existing state."""
    import os
    import psycopg2

    connection = psycopg2.connect(
        host="postgres",
        port=5432,
        dbname="traffic_db",
        user=os.environ["TRAFFIC_DB_USER"],
        password=os.environ["TRAFFIC_DB_PASSWORD"],
        connect_timeout=10,
    )

    try:
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO public.spark_job_execution_log
                        (airflow_run_id, spark_app_name, submitter_name, status)
                    VALUES (%s, %s, %s, 'PENDING')
                    ON CONFLICT (airflow_run_id) DO NOTHING
                    """,
                    (airflow_run_id, app_name, submitter_name),
                )

                cursor.execute(
                    """
                    SELECT spark_app_name, submitter_name, status
                    FROM public.spark_job_execution_log
                    WHERE airflow_run_id = %s
                    """,
                    (airflow_run_id,),
                )

                record = cursor.fetchone()

                if record is None:
                    raise RuntimeError("Spark execution registration failed")

                if record[0] != app_name or record[1] != submitter_name:
                    raise RuntimeError(
                        "Spark execution ID conflict: existing names differ"
                    )

                return record
    finally:
        connection.close()


def update_spark_execution_status(airflow_run_id, status):
    """Update the status of a registered Spark execution."""
    import os
    import psycopg2

    allowed_statuses = {
        "PENDING", "SUBMITTED", "RUNNING", "SUCCEEDED", "FAILED"
    }

    if status not in allowed_statuses:
        raise ValueError(f"Invalid Spark execution status: {status}")

    connection = psycopg2.connect(
        host="postgres",
        port=5432,
        dbname="traffic_db",
        user=os.environ["TRAFFIC_DB_USER"],
        password=os.environ["TRAFFIC_DB_PASSWORD"],
        connect_timeout=10,
    )

    try:
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE public.spark_job_execution_log
                    SET status = %s,
                        updated_at = NOW()
                    WHERE airflow_run_id = %s
                      AND status NOT IN ('SUCCEEDED', 'FAILED')
                    """,
                    (status, airflow_run_id),
                )

                if cursor.rowcount != 1:
                    raise RuntimeError(
                        "Spark execution record missing or already finalized"
                    )
    finally:
        connection.close()

def run_traffic_spark_on_kubernetes():
    api = client.CoreV1Api(get_k8s_client())

    from airflow.sdk import get_current_context
    import hashlib

    context = get_current_context()
    airflow_run_id = context["run_id"]

    run_id = hashlib.sha256(
        airflow_run_id.encode("utf-8")
    ).hexdigest()[:16]

    app_name = f"traffic-airflow-{run_id}"
    submitter_name = f"traffic-submit-{run_id}"

    # Check whether this DAG Run was previously registered.
    existing_execution_record = get_spark_execution_record(
        airflow_run_id
    )

    execution_record = register_spark_execution(
        airflow_run_id,
        app_name,
        submitter_name,
    )

    is_first_execution = existing_execution_record is None

    previous_status = execution_record[2]

    log.info(
        "Spark execution record: run_id=%s status=%s",
        airflow_run_id,
        previous_status,
    )

    if previous_status == "SUCCEEDED":
        log.info(
            "Spark execution already succeeded; skipping submission: %s",
            app_name,
        )
        return {
            "application": app_name,
            "status": "ALREADY_SUCCEEDED",
        }

    if previous_status == "FAILED":
        raise RuntimeError(
            f"Previous Spark execution failed; manual review required: {app_name}"
        )

    command = [
        "/opt/spark/bin/spark-submit",
        "--master", "k8s://https://kubernetes.default.svc:443",
        "--deploy-mode", "cluster",
        "--name", app_name,
        "--conf", "spark.kubernetes.namespace=spark",
        "--conf", f"spark.kubernetes.container.image={SPARK_IMAGE}",
        "--conf", "spark.kubernetes.container.image.pullPolicy=IfNotPresent",
        "--conf", "spark.kubernetes.authenticate.driver.serviceAccountName=spark-driver",
        "--conf", "spark.executor.instances=1",
        "--conf", "spark.driver.memory=1g",
        "--conf", "spark.executor.memory=1g",
        "--conf", "spark.kubernetes.driverEnv.TRAFFIC_JDBC_URL=jdbc:postgresql://host.docker.internal:5433/traffic_db",
        "--conf", "spark.kubernetes.driver.secretKeyRef.TRAFFIC_DB_USER=traffic-postgres-secret:TRAFFIC_DB_USER",
        "--conf", "spark.kubernetes.driver.secretKeyRef.TRAFFIC_DB_PASSWORD=traffic-postgres-secret:TRAFFIC_DB_PASSWORD",
        "--conf", "spark.kubernetes.executor.secretKeyRef.TRAFFIC_DB_USER=traffic-postgres-secret:TRAFFIC_DB_USER",
        "--conf", "spark.kubernetes.executor.secretKeyRef.TRAFFIC_DB_PASSWORD=traffic-postgres-secret:TRAFFIC_DB_PASSWORD",
        "local:///opt/traffic/spark/write_summary.py",
    ]

    pod = client.V1Pod(
        metadata=client.V1ObjectMeta(
            name=submitter_name,
            namespace=NAMESPACE,
            labels={
                "app": "traffic-airflow-submit",
                "traffic-run-id": run_id,
            },
        ),
        spec=client.V1PodSpec(
            service_account_name="spark-driver",
            restart_policy="Never",
            containers=[
                client.V1Container(
                    name="spark-submit",
                    image=SPARK_IMAGE,
                    image_pull_policy="IfNotPresent",
                    command=command,
                )
            ],
        ),
    )

    from kubernetes.client.exceptions import ApiException

    existing_drivers = api.list_namespaced_pod(
        namespace=NAMESPACE,
        label_selector=f"spark-role=driver,spark-app-name={app_name}",
    ).items

    if len(existing_drivers) > 1:
        raise RuntimeError(
            f"Multiple existing Spark Drivers found: {app_name}"
        )

    driver_already_exists = bool(existing_drivers)

    if driver_already_exists:
        log.info(
            "Existing Spark Driver found: %s",
            existing_drivers[0].metadata.name,
        )

    # SPARK_SUBMISSION_RECOVERY_V2
    submitter_exists = False

    try:
        api.read_namespaced_pod(
            name=submitter_name,
            namespace=NAMESPACE,
        )
        submitter_exists = True
        log.info("Reusing existing Submitter: %s", submitter_name)

    except ApiException as exc:
        if exc.status != 404:
            raise

    if not submitter_exists and not driver_already_exists:
        if not is_first_execution:
            raise RuntimeError(
                "SPARK_DUPLICATE_SUBMISSION_BLOCKED: "
                f"Execution record exists but no Kubernetes Pods found: {app_name}"
            )
        log.info("Creating Spark Submitter: %s", submitter_name)

        try:
            api.create_namespaced_pod(
                namespace=NAMESPACE,
                body=pod,
            )
            submitter_exists = True

        except ApiException as exc:
            if exc.status != 409:
                raise

            # Verify that the conflicting Pod really exists.
            api.read_namespaced_pod(
                name=submitter_name,
                namespace=NAMESPACE,
            )
            submitter_exists = True
            log.info("Submitter already created: %s", submitter_name)

    if driver_already_exists and not submitter_exists:
        log.info(
            "Driver exists without Submitter; resuming monitoring: %s",
            app_name,
        )

    # Record submission only after a Submitter or Driver is confirmed.
    if submitter_exists or driver_already_exists:
        if previous_status == "PENDING":
            update_spark_execution_status(
                airflow_run_id,
                "SUBMITTED",
            )
            log.info("SPARK_EXECUTION_SUBMITTED: %s", app_name)

    deadline = time.monotonic() + TIMEOUT_SECONDS
    submitter_succeeded = False
    driver_name = None

    while time.monotonic() < deadline:
        try:
            submitter = api.read_namespaced_pod(
                name=submitter_name,
                namespace=NAMESPACE,
            )
            submitter_phase = submitter.status.phase

        except ApiException as exc:
            if exc.status != 404:
                raise

            submitter_phase = "Missing"

            log.warning(
                "Submitter Pod missing; checking Spark Driver: %s",
                submitter_name,
            )

        if submitter_phase == "Succeeded":
            submitter_succeeded = True

        drivers = api.list_namespaced_pod(
            namespace=NAMESPACE,
            label_selector=(
                f"spark-role=driver,spark-app-name={app_name}"
            ),
        ).items

        if len(drivers) > 1:
            raise RuntimeError(
                f"Multiple Driver Pods found for {app_name}"
            )

        if drivers:
            driver = drivers[0]
            driver_name = driver.metadata.name
            driver_phase = driver.status.phase

            if driver_phase == "Running":
                current_record = get_spark_execution_record(airflow_run_id)
                if current_record and current_record[2] in ("PENDING", "SUBMITTED"):
                    update_spark_execution_status(
                        airflow_run_id,
                        "RUNNING",
                    )
                    log.info("SPARK_EXECUTION_RUNNING: %s", app_name)

            log.info(
                "Application=%s Submitter=%s Driver=%s Phase=%s",
                app_name,
                submitter_phase,
                driver_name,
                driver_phase,
            )

            if driver_phase == "Failed":
                log_failed_pod(api, driver_name)
                update_spark_execution_status(
                    airflow_run_id,
                    "FAILED",
                )
                # SPARK_DRIVER_FAILED_STATUS_RECORDED
                raise RuntimeError(
                    f"Spark Driver failed: {driver_name}"
                )

            if driver_phase == "Succeeded":
                update_spark_execution_status(
                    airflow_run_id,
                    "SUCCEEDED",
                )
                log.info(
                    "SPARK_K8S_TRAFFIC_PIPELINE_SUCCESS: %s",
                    app_name,
                )
                return {
                    "application": app_name,
                    "driver": driver_name,
                    "status": "SUCCEEDED",
                }

        else:
            if submitter_phase == "Failed":
                log_failed_pod(api, submitter_name)
                raise RuntimeError(
                    f"Spark Submitter failed before Driver appeared: {submitter_name}"
                )

            log.info(
                "Waiting for Driver: %s (Submitter=%s)",
                app_name,
                submitter_phase,
            )

        time.sleep(POLL_SECONDS)

    log_failed_pod(api, submitter_name)
    log_failed_pod(api, driver_name)

    # Clean up only Pods belonging to this Spark application.
    # The unique app_name prevents deletion of unrelated Spark jobs.
    try:
        pods = api.list_namespaced_pod(
            namespace=NAMESPACE,
            label_selector=f"spark-app-name={app_name}",
        ).items

        for pod in pods:
            log.warning(
                "Cleaning up timed-out Spark Pod: %s",
                pod.metadata.name,
            )
            api.delete_namespaced_pod(
                name=pod.metadata.name,
                namespace=NAMESPACE,
                grace_period_seconds=30,
            )

        api.delete_namespaced_pod(
            name=submitter_name,
            namespace=NAMESPACE,
            grace_period_seconds=30,
        )

    except Exception as exc:
        log.error(
            "Spark timeout cleanup failed: %s",
            type(exc).__name__,
        )

    raise TimeoutError(
        f"Spark job did not finish within {TIMEOUT_SECONDS}s: "
        f"{app_name}, driver={driver_name}"
    )



def validate_traffic_summary():
    import psycopg2

    sql = """
    WITH expected AS (
        SELECT datum, source_type, kfz_klasse,
               COUNT(*) AS expected_count
        FROM public.traffic_raw
        GROUP BY datum, source_type, kfz_klasse
    ),
    actual AS (
        SELECT datum, source_type, kfz_klasse,
               vehicle_count AS actual_count
        FROM public.traffic_daily_summary
    ),
    comparison AS (
        SELECT
            e.expected_count,
            a.actual_count
        FROM expected e
        FULL OUTER JOIN actual a
          ON e.datum = a.datum
         AND e.source_type = a.source_type
         AND e.kfz_klasse = a.kfz_klasse
    )
    SELECT
        COUNT(*) AS total_groups,
        COUNT(*) FILTER (
            WHERE expected_count IS DISTINCT FROM actual_count
        ) AS mismatch_groups,
        COALESCE(SUM(expected_count), 0) AS expected_total,
        COALESCE(SUM(actual_count), 0) AS actual_total
    FROM comparison;
    """

    import os

    connection = psycopg2.connect(
        host="postgres",
        port=5432,
        dbname="traffic_db",
        user=os.environ["TRAFFIC_DB_USER"],
        password=os.environ["TRAFFIC_DB_PASSWORD"],
        connect_timeout=10,
    )

    try:
        with connection.cursor() as cursor:
            cursor.execute(sql)
            total_groups, mismatches, expected, actual = cursor.fetchone()
    finally:
        connection.close()

    log.info(
        "Traffic validation: groups=%s mismatches=%s "
        "expected=%s actual=%s",
        total_groups, mismatches, expected, actual,
    )

    if total_groups == 0 or mismatches != 0 or expected != actual:
        raise ValueError(
            "TRAFFIC_DATA_VALIDATION_FAILED: "
            f"groups={total_groups}, mismatches={mismatches}, "
            f"expected={expected}, actual={actual}"
        )

    log.info("TRAFFIC_DATA_VALIDATION_SUCCESS")


class TrafficSparkOperator(PythonOperator):
    """PythonOperator with best-effort Spark Pod cleanup on termination."""

    def execute(self, context):
        import hashlib

        run_hash = hashlib.sha256(
            context["run_id"].encode("utf-8")
        ).hexdigest()[:16]

        self._spark_app_name = f"traffic-airflow-{run_hash}"
        self._spark_submitter_name = f"traffic-submit-{run_hash}"

        return super().execute(context)

    def on_kill(self):
        # ON_KILL_PRESERVE_SPARK_PODS
        # Preserve the Spark application for safe retry and recovery.
        app_name = getattr(self, "_spark_app_name", None)

        log.warning(
            "Airflow Task termination requested. "
            "Preserving Kubernetes Spark Pods for recovery: %s",
            app_name,
        )

with DAG(
    dag_id="traffic_kubernetes_pipeline",
    description="Real traffic aggregation using Spark on Kubernetes",
    start_date=datetime(2026, 10, 1, tzinfo=timezone.utc),
    schedule=None,
    max_active_runs=1,
    catchup=False,
    tags=["traffic", "spark", "kubernetes", "batch"],
) as dag:

    submit_and_monitor_spark = TrafficSparkOperator(
        task_id="submit_and_monitor_spark",
        python_callable=run_traffic_spark_on_kubernetes,
        execution_timeout=__import__("datetime").timedelta(minutes=20),
    )

    validate_summary = PythonOperator(
        task_id="validate_traffic_summary",
        python_callable=validate_traffic_summary,
    )

    submit_and_monitor_spark >> validate_summary