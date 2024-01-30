#!/usr/bin/env python3

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col, udf, count, desc
import time

# Initialize session
spark = SparkSession \
    .builder \
    .appName("DF query 2 execution") \
    .getOrCreate()

# Custom function to extract part of date from military time
def get_part_of_day(milit_time):
    if  500 <= milit_time < 1200: return "Morning"
    if 1200 <= milit_time < 1700: return "Afternoon"
    if 1700 <= milit_time < 2100: return "Evening"
    return "Night"

# Get the corresponding UDF
gpod_udf = udf(get_part_of_day, StringType())

# Start measuring time
start_time = time.time()

# Define the schema
crime_schema = StructType([
    StructField("Something00", StringType()),
    StructField("Something01", StringType()),
    StructField("Something02", StringType()),
    StructField(   "TIME OCC", StringType()),
    StructField("Something04", StringType()),
    StructField("Something05", StringType()),
    StructField("Something06", StringType()),
    StructField("Something07", StringType()),
    StructField("Something08", StringType()),
    StructField("Something09", StringType()),
    StructField("Something10", StringType()),
    StructField("Something11", StringType()),
    StructField("Something12", StringType()),
    StructField("Something13", StringType()),
    StructField("Something14", StringType()),
    StructField("Premis Desc", StringType())
])

# Import datasets
folder_path = "hdfs://okeanos-manager:54310/user/mark_deligiannis/data/"
crime_df1 = spark.read.csv(folder_path + "crime_data_1.csv", header=True, schema=crime_schema)
crime_df2 = spark.read.csv(folder_path + "crime_data_2.csv", header=True, schema=crime_schema)

# Select the two important columns
crime_df1 = crime_df1.select("TIME OCC", "Premis Desc")
crime_df2 = crime_df2.select("TIME OCC", "Premis Desc")

# Merge datasets, keep street crimes, apply our custom function to get
# the part of day, get the #Crimes per part of day, and sort results
crime_df = crime_df1.union(crime_df2) \
                    .filter(col("Premis Desc") == "STREET") \
                    .drop("Premis Desc") \
                    .withColumn("Part Of Day", gpod_udf(col("TIME OCC").cast("int"))) \
                    .groupBy("Part Of Day") \
                    .agg(count("*").alias("Number of Crimes")) \
                    .orderBy(desc("Number of Crimes"))

# Print results
crime_df.show()
print("Time elapsed:", time.time() - start_time)