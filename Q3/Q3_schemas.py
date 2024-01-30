#!/usr/bin/env python3

from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Crime data schema
crime_schema = StructType([
    StructField("Something00", StringType()),
    StructField("Something01", StringType()),
    StructField(   "DATE OCC", StringType()),
    StructField("Something03", StringType()),
    StructField("Something04", StringType()),
    StructField("Something05", StringType()),
    StructField("Something06", StringType()),
    StructField("Something07", StringType()),
    StructField("Something08", StringType()),
    StructField("Something09", StringType()),
    StructField("Something10", StringType()),
    StructField("Something11", StringType()),
    StructField("Something12", StringType()),
    StructField("Vict Descent",StringType()),
    StructField("Something14", StringType()),
    StructField("Something15", StringType()),
    StructField("Something16", StringType()),
    StructField("Something17", StringType()),
    StructField("Something18", StringType()),
    StructField("Something19", StringType()),
    StructField("Something20", StringType()),
    StructField("Something21", StringType()),
    StructField("Something22", StringType()),
    StructField("Something23", StringType()),
    StructField("Something24", StringType()),
    StructField("Something25", StringType()),
    StructField(        "LAT", DoubleType()),
    StructField(        "LON", DoubleType())
])

# Reverse geocoding schema
revg_schema = StructType([
    StructField("LAT",     DoubleType()),
    StructField("LON",     DoubleType()),
    StructField("ZIPcode", StringType())
])

# LA income schema
LAincome_schema = StructType([
    StructField("ZIPcode",   StringType()),
    StructField("Community", StringType()),
    StructField("Income",    StringType())
])