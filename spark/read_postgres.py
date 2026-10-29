import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import count

spark = (
    SparkSession.builder
    .appName("TrafficPostgresAnalysis")
    .master("local[2]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

try:
    print("=== Reading PostgreSQL Traffic Data ===")

    df = (
        spark.read
        .format("jdbc")
        .option("url", "jdbc:postgresql://postgres:5432/traffic_db")
        .option("dbtable", "public.traffic_raw")
        .option("user", "traffic_user")
        .option("password", os.environ["TRAFFIC_DB_PASSWORD"])
        .option("driver", "org.postgresql.Driver")
        .load()
    )

    print("=== Total Records ===")
    print(f"Total rows: {df.count()}")

    print("=== Schema ===")
    df.printSchema()

    print("=== Records by Source Type ===")
    result = (
        df.groupBy("source_type")
        .agg(count("*").alias("record_count"))
        .orderBy("source_type")
    )
    result.show(truncate=False)

finally:
    spark.stop()
