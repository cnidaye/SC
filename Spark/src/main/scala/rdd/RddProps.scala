package rdd

import org.apache.spark.{SparkConf, SparkContext}

object RddProps {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("123").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(1 to 10)
    val rdd1 = rdd.map(_ + 1)

    println(rdd1.partitioner)
//    println(rdd1.preferredLocations(Partiiti))
  }

}
