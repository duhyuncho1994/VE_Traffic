
import os
import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as spark_sum


JDBC_URL = "jdbc:postgresql://postgres:5432/traffic_db"

DB_USER = os.getenv("TRAFFIC_DB_USER", "traffic_user")
DB_PASSWORD = os.environ["TRAFFIC_DB_PASSWORD"]


spark = (
    SparkSession.builder
    .appName("TrafficSyntheticBenchmark")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

print("=== BENCHMARK START ===", flush=True)
print(f"Spark Master: {spark.sparkContext.master}", flush=True)
print(
    f"Application ID: {spark.sparkContext.applicationId}",
    flush=True
)

start_time = time.perf_counter()

try:
    df = (
        spark.read
        .format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", "public.traffic_synthetic")
        .option("user", DB_USER)
        .option("password", DB_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .option("partitionColumn", "id")
        .option("lowerBound", "1")
        .option("upperBound", "1000000")
        .option("numPartitions", "4")
        .load()
    )

    summary = (
        df.groupBy(
            "datum",
            "source_type",
            "kfz_klasse"
        )
        .agg(
            spark_sum("vehicle_count").alias(
                "total_vehicles"
            )
        )
    )

    result = summary.collect()

    elapsed = time.perf_counter() - start_time

    total_vehicles = sum(
        row["total_vehicles"] for row in result
    )

    print("=== BENCHMARK RESULT ===", flush=True)
    print("Input rows expected: 1000000", flush=True)
    print(f"Aggregated groups: {len(result)}", flush=True)
    print(f"Total vehicles: {total_vehicles}", flush=True)
    print(f"Elapsed seconds: {elapsed:.3f}", flush=True)
    print("=== BENCHMARK SUCCESS ===", flush=True)

finally:
    spark.stop()
