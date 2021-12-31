package sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

case class grade(id:String, name:String, grade:String)

object GroupTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("groupTest").master("local[4]").getOrCreate()

    val csv = spark.read
      .csv("D:\\MyProject\\SC\\Data\\tourist.csv")
      .toDF("id","name","grade")

    csv.printSchema()
    csv.show()

    import spark.implicits._
    csv.groupBy($"id")
      .agg( count($"id").as("totalNum"), count( when($"grade" > 7, true)).as("passNum") )
      .show()

  }

}
