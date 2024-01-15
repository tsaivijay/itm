package main.scala.chapter3
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object Example3_7 {
    def main(args: Array[String]) {

        val spark = SparkSession
          .builder
          .appName("Example3_7")
          .getOrCreate()
          
        if (args.length <= 0){
            println("usage Example3_7 <file path to blogs.json>")
            System.exit(1)
        } // end of if

        val jsonFile = args(0)
        val schema = StructType(Array(StructField("Id",IntegerType, false), 
            StructField("First", StringType, false),
            StructField("Last",StringType, false),
            StructField("Published",StringType, false),
            StructField("Hits",IntegerType, false),
            StructField("Campaigns", ArrayType(StringType), false)))

        val blogsDF = spark.read.schema(schema).json(jsonFile)
        blogsDF.show(false)
        println(blogsDF.printSchema)
        println(blogsDF.schema)
        
    } // end of def main
} // end of object

