package df

import org.apache.spark.sql.SparkSession

object SparkDataFrame {


  val spark = SparkSession.builder()
    .appName("testDataFrame")
    .master("local[2]")
    .config("spark.eventLog.enabled", "true")
    .config("spark.eventLog.dir", "/home/alexander/for_spark/log/")
//    .config("spark.driver.cores", 1)
//    .config("sparl.driver.memory", "1G")
//    .config("spark.executor.cores", 1)
//    .config("spark.executor.memory", "1G")
//    .config("spark.num-executor", 2)
    .getOrCreate()

   // def main(args: Array[String]): Unit = {


      val df = spark.read.format("parquet").load("/home/alexander/test")

      println(df.show)

      println(s"Count rows ${df.count()}")

      df.write.format("csv").option("sep", ";").save("/home/alexander/for_spark/df.csv")



   // }

}
