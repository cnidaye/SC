package test

import org.apache.spark.{SparkConf, SparkContext}

class MyClass {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("123")

    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(1 to 1000000)

    rdd.map(x => (x%2,1))
      .reduceByKey(_ + _)

  }

}
