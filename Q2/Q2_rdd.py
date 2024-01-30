#!/usr/bin/env python3

from pyspark.sql import SparkSession
from operator import add
import csv
import time

# Initialize session
spark = SparkSession \
    .builder \
    .appName("RDD query 2 execution") \
    .getOrCreate() \
    .sparkContext

# Custom function for map to select the military time
# and the place where the crime took place
TIME_OCC_ind, Premis_Desc_ind = 3, 15
def select_time_and_place(x):
    reader = csv.reader([x], delimiter=',')
    first_row = next(reader)
    return (first_row[TIME_OCC_ind],first_row[Premis_Desc_ind])

# Custom function to extract part of date from military time
def get_part_of_day(milit_time):
        if  500 <= milit_time < 1200: return "Morning"
        if 1200 <= milit_time < 1700: return "Afternoon"
        if 1700 <= milit_time < 2100: return "Evening"
        return "Night"

# Start measuring time
start_time = time.time()

# Import datasets
folder_path = "hdfs://okeanos-manager:54310/user/mark_deligiannis/data/"
crime_rdd1 = spark.textFile(folder_path + "crime_data_1.csv")
crime_rdd2 = spark.textFile(folder_path + "crime_data_2.csv")

# Remove the header and keep the two important columns
header = crime_rdd1.first()
crime_rdd1 = crime_rdd1.filter(lambda row: row != header) \
                       .map(select_time_and_place)
crime_rdd2 = crime_rdd2.filter(lambda row: row != header) \
                       .map(select_time_and_place)

# Merge datasets, keep street crimes, apply our custom function to get
# the part of day, get the #Crimes per part of day, and sort results
crime_rdd  = crime_rdd1.union(crime_rdd2) \
                       .filter(lambda x: x[1] == "STREET") \
                       .map(lambda x: (get_part_of_day(int(x[0])), 1)) \
                       .reduceByKey(add) \
                       .sortBy(keyfunc=lambda x: x[1], ascending=False) \
                       .collect()

# Print results
header, ending = \
"""+-----------+----------------+
|Part Of Day|Number of Crimes|
+-----------+----------------+""", \
"+-----------+----------------+"
left_space, right_space = 11, 16

print(header)
for x, y in crime_rdd:
    y = str(y)
    x_len, y_len = len(x), len(y)
    print("|" + (left_space-x_len)*" " + x + "|" + (right_space-y_len)*" " + y + "|")
print(ending)

print(time.time() - start_time)