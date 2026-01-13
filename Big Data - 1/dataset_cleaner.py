from pyspark.sql import SparkSession 
from pyspark.sql.functions import col, to_timestamp
import argparse 

parser = argparse.ArgumentParser()

parser.add_argument("--input_file", required=True, help="Path to the input CSV file")
parser.add_argument("--output_dir", required=True, help="Path to the output directory")

args = parser.parse_args()

input_file = args.input_file
output_dir = args.output_dir

spark = SparkSession.builder \
    .appName("Big Data") \
    .master("local[*]") \
    .getOrCreate()

df = spark.read.csv(input_file,
    header=True,
    inferSchema=True)

df = df.dropna()

df = df.withColumn(
    "Start_Time",
    to_timestamp(col("Start_Time"), "yyyy-MM-dd HH:mm:ss")
).withColumn(
    "End_Time",
    to_timestamp(col("End_Time"), "yyyy-MM-dd HH:mm:ss")
)

rename_dict = {
    'Distance(mi)': 'Distance_mi',
    'Temperature(F)': 'Temperature_F',
    'Wind_Chill(F)': 'Wind_Chill_F',
    'Humidity(%)': 'Humidity_percent',
    'Pressure(in)': 'Pressure_in',
    'Visibility(mi)': 'Visibility_mi',
    'Wind_Speed(mph)': 'Wind_Speed_mph',
    'Precipitation(in)': 'Precipitation_in'
}

for old_name, new_name in rename_dict.items():
    df = df.withColumnRenamed(old_name, new_name)

df.coalesce(1).write \
  .mode("overwrite") \
  .option("header", True) \
  .csv(output_dir)