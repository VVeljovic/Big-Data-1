import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, lit, avg, count, stddev,min, max, variance

parameter_name = sys.argv[1] #used to count
parameter_name_2 = sys.argv[2] # used to calculate
start_date = sys.argv[3]
end_date   = sys.argv[4]


spark = SparkSession.builder \
    .appName("Big Data") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")


local_path = "./local_dataset/local_dataset.csv"
df = spark.read \
    .format("csv") \
    .option("header", "true") \
    .load(local_path)

# broj nesreca grupisano po ulaznom parametru u odredjenom periodu
df.filter(
    (col("Start_Time") >= to_timestamp(lit(start_date))) &
    (col("End_Time") <= to_timestamp(lit(end_date)))
).groupBy(parameter_name) \
 .agg(count("*").alias("broj_nezgoda")) \
 .show()


df.groupBy(parameter_name).agg(
    min(col(parameter_name_2)).alias(f"{parameter_name_2}_min"),
    max(col(parameter_name_2)).alias(f"{parameter_name_2}_max"),
    avg(col(parameter_name_2)).alias(f"{parameter_name_2}_avg"),
    stddev(col(parameter_name_2)).alias(f"{parameter_name_2}_stddev"),
    variance(col(parameter_name_2)).alias(f"{parameter_name_2}_variance")
).show()

spark.stop()
