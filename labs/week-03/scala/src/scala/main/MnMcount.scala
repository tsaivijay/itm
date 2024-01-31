// Import necessary Spark classes
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

// Define the MnMcount object
object MnMcount {
  // Define the main method
  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession.builder.appName("MnMCount").getOrCreate()

    // Check if the correct number of arguments is provided
    if (args.length < 1) {
      println("Usage: MnMcount <mnm_file_dataset>")
      sys.exit(1)
    }

    // Get the M&M data set file name
    val mnmFile = args(0)

    // Read the file into a Spark DataFrame
    val mnmDF = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(mnmFile)

    // Display DataFrame
    mnmDF.show(5, false)

    // Aggregate count of all colors and groupBy state and color
    // OrderBy descending order
    val countMnMDF = mnmDF.select("State", "Color", "Count")
      .groupBy("State", "Color")
      .sum("Count")
      .orderBy(desc("sum(Count)"))

    // Show all the resulting aggregation for all the dates and colors
    countMnMDF.show(60)
    println(s"Total Rows = ${countMnMDF.count()}")
    println()

    // Find the aggregate count for California by filtering
    val caCountMnNDF = mnmDF.select("*")
      .where(col("State") === "CA")
      .groupBy("State", "Color")
      .sum("Count")
      .orderBy(desc("sum(Count)"))

    // Show the resulting aggregation for California
    caCountMnNDF.show(10)
  }
}

// Call the main method with the provided argument
MnMcount.main(Array("C:/Users/USER/Documents/itmd-521/LearningSparkV2/chapter2/scala/data/mnm_dataset.csv"))
