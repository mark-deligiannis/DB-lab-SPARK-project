#!/usr/bin/env python3

from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType
import time

# Initialize session
spark = SparkSession \
            .builder \
            .appName("SQL query 1 execution") \
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
crime_1 = spark.read.format('csv') \
                    .options(header='true') \
                    .schema(crime_schema) \
                    .load(folder_path + "crime_data_1.csv")
crime_2 = spark.read.format('csv') \
                    .options(header='true') \
                    .schema(crime_schema) \
                    .load(folder_path + "crime_data_2.csv")

# Create a temporary view so that we can query the tables using SQL
crime_1.createOrReplaceTempView("crime_table1")
crime_2.createOrReplaceTempView("crime_table2")

# Merge datasets and select the two important columns
datetime_format = "MM/dd/yyyy hh:mm:ss a"
crime = spark.sql(f"""
    SELECT
        DR_NO, TO_DATE(`DATE OCC`, "{datetime_format}") AS `date`
    FROM
        crime_table1
    UNION ALL
    SELECT
        DR_NO, TO_DATE(`DATE OCC`, "{datetime_format}") AS `date`
    FROM
        crime_table2
""")

# Create a temporary SQL view
crime.createOrReplaceTempView("crime_data_view")

# Get crime count per year,month pair
result = spark.sql("""
    SELECT
        year(date) AS year,
        month(date) AS month,
        COUNT(DR_NO) AS crime_total
    FROM
        crime_data_view
    GROUP BY
        year, month
""")

result.createOrReplaceTempView("result_data_view")

# Use windows to partition by year
result_with_rank = spark.sql("""
    SELECT
        year,
        month,
        crime_total,
        ROW_NUMBER() OVER (PARTITION BY year ORDER BY crime_total DESC) AS `#`
    FROM
        result_data_view
""")

result_with_rank.createOrReplaceTempView("result_data_with_rank")

# Get top 3 months in terms of #crimes
total_crimes = spark.sql("""
    SELECT
        year,
        month,
        crime_total,
        `#`
    FROM
        result_data_with_rank
    WHERE
        `#` <= 3
""")

# Print the result
total_crimes.show(72)
print("Time elapsed:", time.time() - start_time)