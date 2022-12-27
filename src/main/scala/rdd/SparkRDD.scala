package rdd

import org.apache.spark.{SparkConf, SparkContext}

object SparkRDD {

  val conf = new SparkConf()
    .setAppName("testRDD")
    .setMaster("local[2]")

  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(conf)

//    sc.textFile("/home/alexander/for_spark/a.txt")
//      .flatMap(line => line.split(" "))
//      .map(word => (word, 1))
//      .reduceByKey({case (a, b) => a + b})
//      .saveAsTextFile("/home/alexander/for_spark/wc.txt")

//    sc.textFile("/home/alexander/for_spark/a.txt")
//      .flatMap(line => line.split(" "))
//      .map(word => word + "=)")
//      .saveAsTextFile("/home/alexander/for_spark/a1.txt")


    val a = sc.textFile("/home/alexander/for_spark/a.txt")
          .flatMap(line => line.split(" "))
          .map(word => word + "=)")
          .foreach(line => println(line))

    //println(a.toString())

  }

}
