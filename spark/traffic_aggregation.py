import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import count, col

spark = (
    SparkSession.builder
    .appName("TrafficAggregation")
    .master("local[2]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

try:
    df = (
        spark.read
        .format("jdbc")
        .option(
            "url",
            "jdbc:postgresql://postgres:5432/traffic_db"
        )
        .option("dbtable", "public.traffic_raw")
        .option("user", "traffic_user")
        .option("password", os.environ["TRAFFIC_DB_PASSWORD"])
        .option("driver", "org.postgresql.Driver")
        .load()
    )

    print("=== Total Records ===")
    print(df.count())

    print("=== Daily Traffic by Vehicle Class ===")

    result = (
        df.groupBy(
            "datum",
            "source_type",
            "kfz_klasse"
        )
        .agg(count("*").alias("vehicle_count"))
        .orderBy(
            col("datum").asc(),
            col("source_type").asc(),
            col("vehicle_count").desc()
        )
    )

    result.show(30, truncate=False)

    print("=== Total Aggregated Groups ===")
    print(result.count())

finally:
    spark.stop()
