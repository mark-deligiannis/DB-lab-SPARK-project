from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import col, year, count, avg, to_date, udf, desc, asc, min as func_min, round as func_round, row_number
from pyspark.sql.window import Window
from math import radians, sin, cos, sqrt, asin
import time
from Q4_schemas import crime_schema, revg_schema, lapd_ps_schema

# Initialize session
spark = SparkSession \
    .builder \
    .appName("DF query 4 execution") \
    .getOrCreate()

# Start measuring time
start_time = time.time()

# Import datasets
folder_path = "hdfs://okeanos-manager:54310/user/mark_deligiannis/data/"
crime_df1 = spark.read.csv(folder_path+"crime_data_1.csv", header=True, schema=crime_schema)
crime_df2 = spark.read.csv(folder_path+"crime_data_2.csv", header=True, schema=crime_schema)
revgecoding = spark.read.csv(folder_path+"revgecoding.csv", header=True, schema=revg_schema)
lapd_ps = spark.read.csv(folder_path+"LAPD_Police_Stations.csv", header=True, schema=lapd_ps_schema)

# 1) Merge datasets, select useful columns and typecast to appropriate types
# 2) Get the cases where firearms were used and filter out "null island"
input_datetime_format = "MM/dd/yyyy hh:mm:ss a"
crime_df = crime_df1.union(crime_df2) \
                    .select("DR_NO", \
                            year(to_date(col("DATE OCC"), input_datetime_format)).alias("year"), \
                            "Weapon Used Cd", "AREA", "LAT", "LON") \
                    .filter((col('LAT') != 0.0) & (col('LON') != 0.0) & col("Weapon Used Cd").like("1%"))

# Reuse code from the "geopy" module to find distance between two points on earth
def haversine_distance(lat1, lon1, lat2, lon2):
        # Convert latitude and longitude from degrees to radians
        lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
        # Haversine formula to calculate distance
        dlat = lat2 - lat1
        dlon = lon2 - lon1
        area = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
        central_angle = 2 * asin(sqrt(area))
        # Earth radius in kilometers
        radius = 6371.0
        # Calculate distance
        distance = radius * central_angle
        return distance

# Get the corresponding UDF
dist_udf = udf(haversine_distance, DoubleType())

# 1) Select useful columns from lapd_ps dataset
# 2) typecast to appropriate types 
lapd_ps = lapd_ps.select("X", "Y", "division", "AREA")

# First Query

# 1) Join the police departments with the crimes based on the area of the police department they were handled by
# 2) Calculate the Harvesine distance between the crime area and the police department area
# 3) Drop the columns with coordinates
crime_df1 = crime_df.join(lapd_ps, ['AREA']) \
                   .withColumn("Distance", dist_udf(col("LAT"),col("LON"),col("Y"),col("X"))) \
                   .drop("LAT", "LON", "Y", "X")

# 1) Group based on year
# 2) Aggregate by counting the crimes per year
# 3) Calculate the average distance between station and crime per year and rounding it to 3 decimal digits
# 4) Show them in ascending order based on year
crime_df1a = crime_df1.groupBy("year") \
                      .agg(func_round(avg("Distance"),3).alias("average_distance"),\
                           count("DR_NO").alias("#")) \
                      .orderBy(col("year").asc())

# 1) Group based on division
# 2) Aggregate by counting the crimes per division
# 3) Calculate the average distance between station and crime per year and rounding it to 3 decimal digits
# 4) Show them in descending order based on number of crimes
crime_df1b = crime_df1.groupBy("division") \
                      .agg(func_round(avg("Distance"),3).alias("average_distance"),\
                           count("DR_NO").alias("#")) \
                      .orderBy(col("#").desc())


# Second Query

# 1) Kartesian product of crime_df and lapd_ps
# 2) Calculate the Harvesine distance between the crime area and the police department area
# 3) Drop the columns with the coordinates
crime_df2 = crime_df.join(lapd_ps) \
                    .withColumn("Distance", dist_udf(col("LAT"), col("LON"), col("Y"), col("X"))) \
                    .drop("LAT", "LON", "Y", "X")

# Create a window partition based on DR_NO
window_spec = Window.partitionBy("DR_NO")

# Use window functions to find the minimum distance for each crime
crime_df2 = crime_df2.withColumn("min_distance", func_min("Distance").over(window_spec))

# Keep the crime only for the stations with the minimum distance from the crime scene
crime_df2 = crime_df2.filter(crime_df2["Distance"]==crime_df2["min_distance"])

# 1) Group based on year
# 2) Aggregate by counting the crimes per year
# 3) Calculate the average distance between station and crime per year and rounding it to 3 decimal digits
# 4) Show them in ascending order based on year
crime_df2a = crime_df2.groupBy("year") \
                      .agg(func_round(avg("Distance"),3).alias("average_distance"),\
                           count("DR_NO").alias("#")) \
                      .orderBy(col("year").asc())

# 1) Group based on division
# 2) Aggregate by counting the crimes per division
# 3) Calculate the average distance between station and crime per year and rounding it to 3 decimal digits
# 4) Show them in descending order based on number of crimes
crime_df2b = crime_df2.groupBy("division") \
                      .agg(func_round(avg("Distance"),3).alias("average_distance"),\
                           count("DR_NO").alias("#")) \
                      .orderBy(col("#").desc())

print("Grouped by year: Number of crimes and Average distance from corresponding Police Station")
crime_df1a.show(30,truncate=False)
print("Grouped by division: Number of crimes and Average distance from corresponding Police Station")
crime_df1b.show(30, truncate=False)
print("Grouped by year: Number of crimes and Average distance from nearest Police Station")
crime_df2a.show(30, truncate=False)
print("Grouped by division: Number of crimes and Average distance from nearest Police Station")
crime_df2b.show(30, truncate=False)

print("Time elapsed:", time.time() - start_time)
