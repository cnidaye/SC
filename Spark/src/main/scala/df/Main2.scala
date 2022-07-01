package df

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object Main2 {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("oee_casetek")
      .getOrCreate()
    spark.sparkContext.setLogLevel("error")
    val data = Seq(
    ("hp",	"2022-02-06",	"2022-02-20", "001"),
    ("hp",	"2022-02-15",	"2022-02-24", "002"),
    ("hp",	"2022-02-20",	"2022-02-22", "003"),
    ("hp",	"2022-02-25",	"2022-02-28", "004")
    )
    val hour = "2022-02-25 08"
    import spark.implicits._

    data.toDF("brand", "start_date", "end_date","code")
      .createOrReplaceTempView("computer_promotion")

    spark.sql(
      """
        | select * from computer_promotion
        |""".stripMargin)
      .withColumn("id", (row_number() over Window.partitionBy($"brand").orderBy($"start_date")).cast("string"))
//      .withColumn("id", lit(hour) + lit(hour))
      .withColumn("id", hash($"brand") + $"id")
//      .printSchema()
      .show(false)

  }

}
