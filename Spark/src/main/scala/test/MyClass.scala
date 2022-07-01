package test

import org.apache.spark.{SparkConf, SparkContext}

object MyClass {
  def main(args: Array[String]): Unit = {
    val a = Array(1,2,3)
    println(a.zipWithIndex.mkString(";"))
  }
}
