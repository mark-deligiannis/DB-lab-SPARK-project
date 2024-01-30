#!/usr/bin/env python3

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date

spark = SparkSession \
    .builder \
    .appName("Format Crime DataFrame") \
    .getOrCreate()

folder_path = "hdfs://okeanos-manager:54310/user/mark_deligiannis/data/"
crime_df1 = spark.read.csv(folder_path + "crime_data_1.csv", header=True)
crime_df2 = spark.read.csv(folder_path + "crime_data_2.csv", header=True)

input_datetime_format = "MM/dd/yyyy hh:mm:ss a"
crime_df = crime_df1.union(crime_df2) \
                    .select(to_date(col("Date Rptd"), input_datetime_format).alias("Date Rptd"), \
                            to_date(col("DATE OCC"), input_datetime_format).alias("DATE OCC"), \
                            col("Vict Age").cast("int"), \
                            col("LAT").cast("double"), \
                            col("LON").cast("double"))

print(f"There are {crime_df.count()} rows in the crime dataset.\nColumn names and datatypes:")
crime_df.printSchema()
