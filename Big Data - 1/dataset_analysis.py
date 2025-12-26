from pyspark.sql import SparkSession 
from pyspark.sql.functions import col, to_timestamp

spark = SparkSession.builder \
    .appName("Big Data") \
    .master("local[*]") \
    .getOrCreate()

df = spark.read.csv("./US_Accidents_March23.csv",
    header=True,
    inferSchema=True)

df = df.dropna(subset=["Start_Time",
                   "End_Time", 
                   "Distance(mi)", 
                   "City",
                   "Country",
                   "Temperature(F)",
                   "Humidity(%)",
                   "Pressure(in)",
                   "Visibility(mi)",
                   "Wind_Speed(mph)",
                   "Weather_Condition"])

df = df.withColumn(
    "Start_Time",
    to_timestamp(col("Start_Time"), "yyyy-MM-dd HH:mm:ss")
).withColumn(
    "End_Time",
    to_timestamp(col("End_Time"), "yyyy-MM-dd HH:mm:ss")
)

df.coalesce(1).write \
  .mode("overwrite") \
  .option("header", True) \
  .csv("final_dataset")