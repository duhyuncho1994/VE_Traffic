
import os
import psycopg


DB_CONFIG = {
    "host": os.getenv("TRAFFIC_DB_HOST", "postgres"),
    "port": int(os.getenv("TRAFFIC_DB_PORT", "5432")),
    "dbname": os.getenv("TRAFFIC_DB_NAME", "traffic_db"),
    "user": os.getenv("TRAFFIC_DB_USER", "traffic_user"),
    "password": os.environ["TRAFFIC_DB_PASSWORD"],
}


def record_task_result(context, status):
    task_instance = context["task_instance"]

    dag_id = task_instance.dag_id
    run_id = task_instance.run_id
    task_id = task_instance.task_id

    error_message = None

    if status == "FAILED":
        error = context.get("exception")
        error_message = str(error) if error else "Task failed"

    with psycopg.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO batch_execution_log (
                    dag_id,
                    run_id,
                    task_id,
                    status,
                    error_message
                )
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (dag_id, run_id, task_id)
                DO UPDATE SET
                    status = EXCLUDED.status,
                    error_message = EXCLUDED.error_message,
                    recorded_at = NOW()
                """,
                (
                    dag_id,
                    run_id,
                    task_id,
                    status,
                    error_message,
                ),
            )


def on_task_success(context):
    record_task_result(context, "SUCCESS")


def on_task_failure(context):
    record_task_result(context, "FAILED")
