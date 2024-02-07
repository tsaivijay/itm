from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Create SparkSession
spark = SparkSession.builder \
    .appName("Divvy Trips Analysis") \
    .getOrCreate()

# Define file path
file_path = "Divvy_Trips_2015-Q1.csv"

# First: infer the schema and read the csv file
df1 = spark.read.option("header", "true").csv(file_path)
print("DataFrame with inferred schema:")
df1.printSchema()
print("Number of records:", df1.count())

# Second: programmatically create and attach a schema and read the csv file
schema = StructType([
    StructField("trip_id", IntegerType(), True),
    StructField("starttime", StringType(), True),
    StructField("stoptime", StringType(), True),
    StructField("bikeid", IntegerType(), True),
    StructField("tripduration", IntegerType(), True),
    StructField("from_station_id", IntegerType(), True),
    StructField("from_station_name", StringType(), True),
    StructField("to_station_id", IntegerType(), True),
    StructField("to_station_name", StringType(), True),
    StructField("usertype", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("birthyear", IntegerType(), True)
])
df2 = spark.read.option("header", "true").schema(schema).csv(file_path)
print("\nDataFrame with schema defined programmatically:")
df2.printSchema()
print("Number of records:", df2.count())

# Third: attach a schema via a DDL and read the csv file
ddl_schema = "trip_id INT, starttime STRING, stoptime STRING, bikeid INT, tripduration INT, from_station_id INT, from_station_name STRING, to_station_id INT, to_station_name STRING, usertype STRING, gender STRING, birthyear INT"
df3 = spark.read.option("header", "true").schema(ddl_schema).csv(file_path)
print("\nDataFrame with schema defined via DDL:")
df3.printSchema()
print("Number of records:", df3.count())

# Transformations and Actions
df_filtered = df3.select("gender").filter(df3.gender != "null")
df_filtered.show(10)

# Stop SparkSession
spark.stop()
