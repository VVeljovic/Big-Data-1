from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, lit, avg, count, stddev,min, max, variance, sum
import argparse
import time 

start_time = time.perf_counter()

parser = argparse.ArgumentParser()
parser.add_argument("group_by_column", help="Column name to group by")
parser.add_argument("metric_column", help="Column name used for statistical calculations")
parser.add_argument("sum_column", help="Column name to sum over")
parser.add_argument("start_date", help="Start date")
parser.add_argument("end_date", help="End date")
parser.add_argument("input_file", help="Path to the input CSV file")

args = parser.parse_args()

group_by_column = args.group_by_column
metric_column = args.metric_column
sum_column = args.sum_column
start_date = args.start_date
end_date = args.end_date
local_path = args.input_file

spark = SparkSession.builder \
    .appName("Big Data") \
    .master("local[*]") \
    .getOrCreate()

df = spark.read \
    .format("csv") \
    .option("header", "true") \
    .load(local_path)

df_filtered = df.filter(
    (col("Start_Time") >= to_timestamp(lit(start_date))) &
    (col("End_Time") <= to_timestamp(lit(end_date)))
).groupBy(group_by_column) \
 .agg(count("*").alias("broj_nezgoda"))\
 .coalesce(1).write \
 .mode("overwrite") \
 .option("header", "true") \
 .csv("output/filtered_count_by_parameters")

df_summed = (
    df.filter(
        (col("Start_Time") >= start_date) &
        (col("End_Time") <= end_date)
    )
    .groupBy(group_by_column)
    .agg(sum(col(sum_column)).alias(f"{sum_column}_sum"))
).coalesce(1).write \
 .mode("overwrite") \
 .option("header", "true") \
 .csv("output/sum_by_parameters")


df = df.groupBy(group_by_column).agg(
    min(col(metric_column)).alias(f"{metric_column}_min"),
    max(col(metric_column)).alias(f"{metric_column}_max"),
    avg(col(metric_column)).alias(f"{metric_column}_avg"),
    stddev(col(metric_column)).alias(f"{metric_column}_stddev"),
    variance(col(metric_column)).alias(f"{metric_column}_variance")
).coalesce(1).write \
 .mode("overwrite") \
 .option("header", "true") \
 .csv("output/grouped_stats")

spark.stop()

end_time = time.perf_counter()
elapsed_time = end_time - start_time

print(f"Elapsed time: {elapsed_time:.4f} seconds")
