
import os
import re
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import count


JDBC_URL = os.getenv("TRAFFIC_JDBC_URL", "jdbc:postgresql://postgres:5432/traffic_db")

DB_USER = os.getenv(
    "TRAFFIC_DB_USER",
    "traffic_user"
)

DB_PASSWORD = os.environ["TRAFFIC_DB_PASSWORD"]

SQL_FILE = Path(
    "/opt/traffic/sql/merge_traffic_summary.sql"
)


# Spark 실행 환경은 spark-submit에서 지정한다.
# Local Mode 또는 Standalone Cluster Mode 모두 지원.
spark = (
    SparkSession.builder
    .appName("TrafficDailySummaryPipeline")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

print("=== Spark Execution Environment ===")
print(f"Spark Master: {spark.sparkContext.master}")
print(f"Application ID: {spark.sparkContext.applicationId}")


def merge_summary():
    print("=== Executing PostgreSQL UPSERT ===")

    sql = SQL_FILE.read_text(encoding="utf-8-sig")

    # JDBC transaction is managed explicitly below.
    sql = re.sub(
        r"(?im)^\s*(BEGIN|COMMIT)\s*;\s*$",
        "",
        sql
    ).strip()

    jvm = spark._jvm

    properties = jvm.java.util.Properties()
    properties.setProperty("user", DB_USER)
    properties.setProperty("password", DB_PASSWORD)

    driver = jvm.org.postgresql.Driver()
    connection = driver.connect(
        JDBC_URL,
        properties
    )

    if connection is None:
        raise RuntimeError(
            "PostgreSQL JDBC connection failed"
        )

    connection.setAutoCommit(False)

    try:
        statement = connection.createStatement()

        try:
            statement.execute(sql)
            connection.commit()
        finally:
            statement.close()

        print("UPSERT committed successfully")

    except Exception:
        connection.rollback()
        print("UPSERT rolled back")
        raise

    finally:
        connection.close()


try:
    print("=== Reading traffic_raw ===")

    df = (
        spark.read
        .format("jdbc")
        .option("url", JDBC_URL)
        .option(
            "dbtable",
            "public.traffic_raw"
        )
        .option("user", DB_USER)
        .option("password", DB_PASSWORD)
        .option(
            "driver",
            "org.postgresql.Driver"
        )
        .load()
    )

    source_count = df.count()

    print(f"Source records: {source_count}")

    if source_count == 0:
        raise ValueError(
            "Source table is empty"
        )

    print("=== Aggregating traffic data ===")

    summary = (
        df.groupBy(
            "datum",
            "source_type",
            "kfz_klasse"
        )
        .agg(
            count("*").alias("vehicle_count")
        )
    )

    group_count = summary.count()

    print(f"Aggregated groups: {group_count}")

    if group_count == 0:
        raise ValueError(
            "No aggregated data found"
        )

    print("=== Writing staging table ===")

    (
        summary.write
        .format("jdbc")
        .option("url", JDBC_URL)
        .option(
            "dbtable",
            "public.traffic_daily_summary_staging"
        )
        .option("user", DB_USER)
        .option("password", DB_PASSWORD)
        .option(
            "driver",
            "org.postgresql.Driver"
        )
        .mode("overwrite")
        .save()
    )

    print("Staging write completed")

    merge_summary()

    print("=== PIPELINE SUCCESS ===")
    print(f"Source records: {source_count}")
    print(f"Aggregated groups: {group_count}")
    print(f"Spark Master: {spark.sparkContext.master}")

finally:
    spark.stop()
