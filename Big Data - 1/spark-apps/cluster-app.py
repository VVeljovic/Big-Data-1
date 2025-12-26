import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_timestamp, lit,
    avg, count, stddev, min, max, variance
)

parameter_name = sys.argv[1]
parameter_name_2 = sys.argv[2]
start_date = sys.argv[3]
end_date = sys.argv[4]


spark = SparkSession.builder \
    .appName("Big Data") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")


df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("hdfs://namenode:9000/input/cleaned_dataset.csv")


df = df.withColumn(
    "Start_Time",
    to_timestamp(col("Start_Time"), "yyyy-MM-dd HH:mm:ss")
).withColumn(
    "End_Time",
    to_timestamp(col("End_Time"), "yyyy-MM-dd HH:mm:ss")
)


df.filter(
    (col("Start_Time") >= to_timestamp(lit(start_date), "yyyy-MM-dd HH:mm:ss")) &
    (col("End_Time") <= to_timestamp(lit(end_date), "yyyy-MM-dd HH:mm:ss"))
).groupBy(parameter_name) \
 .agg(count("*").alias("broj_nezgoda")) \
 .orderBy(col("broj_nezgoda").desc()) \
 .show(20, truncate=False)


df.groupBy(parameter_name).agg(
    min(col(parameter_name_2)).alias(f"{parameter_name_2}_min"),
    max(col(parameter_name_2)).alias(f"{parameter_name_2}_max"),
    avg(col(parameter_name_2)).alias(f"{parameter_name_2}_avg"),
    stddev(col(parameter_name_2)).alias(f"{parameter_name_2}_stddev"),
    variance(col(parameter_name_2)).alias(f"{parameter_name_2}_variance")
).show(20, truncate=False)

spark.stop()
