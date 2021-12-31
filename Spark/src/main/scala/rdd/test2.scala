package rdd

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object test2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[2]").getOrCreate()

    import spark.implicits._
    val structure = StructType(
      Seq(
        StructField("name",StringType),
        StructField("age",IntegerType)
      )
    )
    val df = spark.read.schema(structure).option("header","true").csv("/home/xy/IdeaProjects/SC/Spark/src/main/scala/rdd/123.txt")
    df.printSchema()
    df.show()

    df.groupBy($"name")
      .mean("age")
      .show()

    val ds = df.as[Person]

    ds.filter(p => p.age > 13)
      .map(_)
  }
}

case class Person(name:String, age:Int)
