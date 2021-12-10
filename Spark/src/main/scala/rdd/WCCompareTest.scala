package rdd

import org.apache.spark.{SparkConf, SparkContext}

object WCCompareTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("test04").setMaster("local[2]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

//    groupByTest(sc)
    reduceTest(sc)
 }

  def groupByTest(sc:SparkContext) = {
    val l1 = System.currentTimeMillis()
    sc.textFile("D:\\MyProject\\SC\\Data\\large_wc.txt")
      .flatMap(_.split(" "))
      .groupBy(x => x)
      .map(x => (x._1, x._2.size))
      .foreach(println)

    val l2 = System.currentTimeMillis()

    println("=====Time cost:" + (l2-l1) + "============")

  }
  def reduceTest(sc:SparkContext) = {
    val l1 = System.currentTimeMillis()
    sc.textFile("D:\\MyProject\\SC\\Data\\large_wc.txt")
      .flatMap(_.split(" "))
      .map(x => (x,1))
      .reduceByKey(_ + _)
      .foreach(println)
    val l2 = System.currentTimeMillis()

    println("=====Time cost:" + (l2-l1) + "==========")

  }
}
