package rdd

import org.apache.spark.{SparkConf, SparkContext}

object SparkRDD {

  val conf = new SparkConf()
    .setAppName("testRDD")
    .setMaster("local[2]")

  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(conf)

    sc.textFile("build.sbt")
      .flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey({case (a, b) => a + b})
      .saveAsTextFile("/home/alexander/wc.txt")

  }

}
