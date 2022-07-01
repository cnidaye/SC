package test

import java.io.File
import scala.io.Source

object LabelTask {
  def main(args: Array[String]): Unit =
    {
      val spark = org.apache.spark.sql.SparkSession.builder().appName("demo").getOrCreate()
      val sc = spark.sparkContext
      sc.setLogLevel("ERROR")

      spark.read.option("header","true").csv("")
    }

  def findIndex1(path:String, label:String) :Int= {
    val source = Source.fromFile(new File(path),"utf-8")
    source.close()
    val lines = source.getLines().toArray
    val map = lines.zipWithIndex.toMap
    map.getOrElse(label, -1)
  }

  def findIndex2(path:String)(label:String):Int = {
    val src = Source.fromFile(new File(path),"utf-8")
    src.close()
    val map = src.getLines().zipWithIndex.toMap
    map.getOrElse(label,-1)
  }


}
