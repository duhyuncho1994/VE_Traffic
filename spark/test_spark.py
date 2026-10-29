
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as spark_sum

spark = (
    SparkSession.builder
    .appName("TrafficSparkTest")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

data = [
    ("KONTROLLE", 100),
    ("KONTROLLE", 150),
    ("TRACKING", 120),
    ("TRACKING", 160),
]

df = spark.createDataFrame(
    data,
    ["source_type", "vehicle_count"]
)

result = df.groupBy("source_type").agg(
    spark_sum("vehicle_count").alias("total_vehicles")
)

print("=== Spark Traffic Aggregation ===")
result.show()

spark.stop()
