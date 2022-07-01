package sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._
object BroadCasetTest {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("broadcast").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().config(conf).getOrCreate()


    val grades = spark.read.option("header","true").csv("D:\\MyProject\\SC\\Data\\grades.csv")

    val id = spark.read.option("header","true").csv("D:\\MyProject\\SC\\Data\\stu.csv")
    val bcDF = broadcast(id)

    import spark.implicits._
    val avg = grades.groupBy($"id")
      .agg((sum("grade")/count("subject")).as("avgGra"))


//    avg.join(bcDF,"id")
//      .select("id","name","avgGra")
//      .show()
    avg.explain(true)


  }

}
