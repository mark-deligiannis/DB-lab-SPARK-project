#!/usr/bin/env python3

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, count as countSp, to_date, row_number
from pyspark.sql.types import StructField, StructType, StringType
from pyspark.sql.window import Window
import time

# Initialize session
spark = SparkSession \
    .builder \
    .appName("DF query 1 execution") \
    .getOrCreate()

# Start measuring time
start_time = time.time()

# Define the schema
crime_schema = StructType([
    StructField("DR_NO", StringType()),
    StructField("DATE RPTD", StringType()),
    StructField("DATE OCC", StringType())
])

# Import datasets
folder_path = "hdfs://okeanos-manager:54310/user/mark_deligiannis/data/"
crime_df1 = spark.read.csv(folder_path + "crime_data_1.csv", header=True, schema=crime_schema)
crime_df2 = spark.read.csv(folder_path + "crime_data_2.csv", header=True, schema=crime_schema)

# Select the two important columns and merge datasets
datetime_format = "MM/dd/yyyy hh:mm:ss a"
crime_df1 = crime_df1.select("DR_NO", to_date(col("DATE OCC"), datetime_format).alias("date"))
crime_df2 = crime_df2.select("DR_NO", to_date(col("DATE OCC"), datetime_format).alias("date"))
crime_df = crime_df1.union(crime_df2)

# Extract year and month from DATE OCC and
# Calculate the number of crimes in each year,month pair
result_df = crime_df.groupBy(year("date").alias("year"),   \
                             month("date").alias("month")) \
                    .agg(countSp("*").alias("crime_total"))

# Partition by year and get top 3 months in terms of #crimes
window_spec = Window.partitionBy("year") \
                    .orderBy(col("crime_total").desc())
total_crimes_df = result_df.withColumn("#", row_number().over(window_spec)) \
                           .filter(col("#") <= 3)

# Print results
total_crimes_df.show(72)
print("Time elapsed:", time.time() - start_time)