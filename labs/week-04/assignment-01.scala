import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType}

object DivvyTripsAnalysis {
  def main(args: Array[String]): Unit = {
    // Create SparkSession
   val spark = SparkSession.builder
  .appName("Divvy Trips Analysis")
  .master("local[*]") // Set the master URL here
  .getOrCreate()

    // Define file path
    val file_path = "Divvy_Trips_2015-Q1.csv"

    // First: infer the schema and read the csv file
    val df1 = spark.read.option("header", "true").csv(file_path)
    println("DataFrame with inferred schema:")
    df1.printSchema()
    println("Number of records:", df1.count())

    // Second: programmatically create and attach a schema and read the csv file
    val schema = new StructType()
      .add("trip_id", IntegerType)
      .add("starttime", StringType)
      .add("stoptime", StringType)
      .add("bikeid", IntegerType)
      .add("tripduration", IntegerType)
      .add("from_station_id", IntegerType)
      .add("from_station_name", StringType)
      .add("to_station_id", IntegerType)
      .add("to_station_name", StringType)
      .add("usertype", StringType)
      .add("gender", StringType)
      .add("birthyear", IntegerType)
    val df2 = spark.read.option("header", "true").schema(schema).csv(file_path)
    println("\nDataFrame with schema defined programmatically:")
    df2.printSchema()
    println("Number of records:", df2.count())

    // Third: attach a schema via a DDL and read the csv file
    val ddl_schema = "trip_id INT, starttime STRING, stoptime STRING, bikeid INT, tripduration INT, from_station_id INT, from_station_name STRING, to_station_id INT, to_station_name STRING, usertype STRING, gender STRING, birthyear INT"
    val df3 = spark.read.option("header", "true").schema(ddl_schema).csv(file_path)
    println("\nDataFrame with schema defined via DDL:")
    df3.printSchema()
    println("Number of records:", df3.count())

    // Transformations and Actions
    val df_filtered = df3.select("gender").filter(df3("gender") =!= "null")
    df_filtered.show(10)

    // Stop SparkSession
    spark.stop()
  }
}
