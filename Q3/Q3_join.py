#!/usr/bin/env python3

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from pyspark.sql.functions import col, year, count, to_date, udf, expr
from Q3_schemas import crime_schema, revg_schema, LAincome_schema
import time

# Initialize session
spark = SparkSession \
        .builder \
        .appName("DF query 3 multiple joins execution") \
        .getOrCreate()

# Start measuring time
start_time = time.time()

# Import datasets
folder_path = "hdfs://okeanos-manager:54310/user/mark_deligiannis/data/"
crime_df = spark.read.csv(folder_path + "crime_data_1.csv", header=True, schema=crime_schema)
revgecoding = spark.read.csv(folder_path + "revgecoding.csv", header=True, schema=revg_schema)
income_2015 = spark.read.csv(folder_path + "income/LA_income_2015.csv", header=True, schema=LAincome_schema)

# Function that changes letters to the race they represent in the dataset
def get_race(descent):
    descent_mapping = {
        "A": "Other Asian",             "B": "Black",                           "C": "Chinese",
        "D": "Cambodian",               "F": "Filipino",                        "G": "Guamanian",
        "H": "Hispanic/Latin/Mexican",  "I": "American Indian/Alaskan Native",  "J": "Japanese",
        "K": "Korean",                  "L": "Laotian",                         "O": "Other",
        "P": "Pacific Islander",        "S": "Samoan",                          "U": "Hawaiian",
        "V": "Vietnamese",              "W": "White",                           "Z": "Asian Indian"
    }
    return descent_mapping[descent]

# Get the corresponding UDF
race_udf = udf(get_race, StringType())

# 1) Select the necessary columns and cast to the proper data type
# 2) Filter out the cases in which there is no victim or the victim is of unknown origin
# 3) Only include data from the year 2015
# 4) Get the full race name from the starting letter
# 5) Drop the columns that are no longer required
input_datetime_format = "MM/dd/yyyy hh:mm:ss a"
crime_df = crime_df.select("DATE OCC", "Vict Descent", "LAT", "LON") \
                   .filter((col("Vict Descent") != '') & (col("Vict Descent") != 'X')) \
                   .withColumn("year", year(to_date(col("DATE OCC"), input_datetime_format))) \
                   .filter(col("year") == 2015) \
                   .withColumn("Vict Descent", race_udf(col("Vict Descent"))) \
                   .drop("DATE OCC", "year")

# If multiple ZIPcodes are recorded, only get one
revgecoding = revgecoding.withColumn("ZIPcode", col("ZIPcode").substr(1,5))

def join_custom(crime_df, revgecoding, income_2015, join_type):

    # Join the datasets based on Latitude and Longtitude values
    joined_df = crime_df.join(revgecoding.hint(join_type), ["LAT","LON"], "inner") \
                        .drop("LAT","LON")

    # 1) Only keep the communities of Los Angeles
    # 2) Convert Income to an integer
    # 3) Drop the columns that are no longer necessary
    income_2015 = income_2015.filter(col("Community").contains("Los Angeles")) \
                             .withColumn("Income Int",expr("CAST(regexp_replace(substring(`Income`, 2), ',', '') AS INT)")) \
                             .drop("Income","Community")
 
    # Order based on the income in a descending order to only keep the 3 maximum communities
    max_income_2015 = income_2015.orderBy(col("Income Int").desc()) \
                                 .limit(3)

    # Perform inner join based on Zip codes to join the datasets and then drop the ZIP codes
    max_final_df = joined_df.join(max_income_2015.hint(join_type), ["ZIPcode"]) \
                            .drop("ZIPcode","Zip code")

    # 1) Group based on Victim's race
    # 2) Count them based on race
    # 3) Sort in descending order of crimes per race group
    max_final_df = max_final_df.groupBy("Vict Descent") \
                               .agg(count("*").alias("#")) \
                               .orderBy(col("#").desc())

    # Order based on the income in an ascending order to only keep the 3 minimum communities
    min_income_2015 = income_2015.orderBy(col("Income Int").asc()) \
                                 .limit(3)

    # Inner join based on Zip codes to join the datasets and then we drop the ZIP codes
    min_final_df = joined_df.join(min_income_2015.hint(join_type), ["ZIPcode"]) \
                            .drop("ZIPcode","Zip code")

    # 1) Group based on Victim's race
    # 2) Count them based on race
    # 3) Sort in descending order of crimes per race group
    min_final_df = min_final_df.groupBy("Vict Descent") \
                               .agg(count("*").alias("#")) \
                               .orderBy(col("#").desc())
    print("Maximum wage")
    max_final_df.show(truncate=False)
    max_final_df.explain()

    print("Minimum wage")
    min_final_df.show(truncate=False)
    min_final_df.explain()

print("==============Broadcast Join===============")
start1 = time.time()
join_custom(crime_df, revgecoding, income_2015, "broadcast")
print("Time needed:", time.time() - start1)
print("\n\n==============Merge Join===============\n")
start2 = time.time()
join_custom(crime_df, revgecoding, income_2015, "merge")
print("Time needed:", time.time() - start2)
print("\n\n==============Shuffle Hash Join===============\n")
start3 = time.time()
join_custom(crime_df, revgecoding, income_2015, "shuffle_hash")
print("Time needed:", time.time() - start3)
print("\n\n==============Shuffle Replicate NL Join===============\n")
start4 = time.time()
join_custom(crime_df, revgecoding, income_2015, "shuffle_replicate_nl")
print("Time needed:", time.time() - start4)
print()
