package stream

import org.apache.spark.sql.SparkSession
//nc -lk 9999
//apache spark
//apache hadoop
object SparkStream {

  def main(args: Array[String]): Unit = {


    val spark = SparkSession.builder()
      .appName("StreamApi")
      .master("local[2]")
      .getOrCreate()

    import spark.implicits._

    val lines = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()

    // Split the lines into words
    val words = lines.as[String].flatMap(_.split(" "))

    // Generate running word count
    val wordCounts = words.groupBy("value").count()

    // Start running the query that prints the running counts to the console
    val query = wordCounts.writeStream
      .outputMode(("complete"))
      .format("console")
      .start()

    query.awaitTermination()
  }
}
