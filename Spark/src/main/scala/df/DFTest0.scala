package df

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.substring

object DFTest0 {
  def main(args: Array[String]): Unit = {
    val spark = org.apache.spark.sql.SparkSession.builder().appName("test").master("local[2]").getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._
    sc.setLogLevel("ERROR")

    val df = spark.read.option("header","true").csv("D:\\MyProject\\SC\\Spark\\src\\main\\scala\\df\\data.csv")

    spark.sql(
      """
        |SELECT TERMINAL_ID, ERROR_CODE, START_TIME, MAX(TIME) END_TIME
        |FROM
        |(
        |SELECT TERMINAL_ID, TIME,
        |LAG(BASE_CODE,1,BASE_CODE) OVER (PARTITION BY TERMINAL_ID ORDER BY TIME) ERROR_CODE,
        |MAX(BASE_TIME) OVER (PARTITION BY TERMINAL_ID ORDER BY TIME ) START_TIME
        |FROM
        | (
        |SELECT
        | TERMINAL_ID, ERROR_CODE, TIME,
        | CASE WHEN SUM(FLAG) OVER (PARTITION BY TERMINAL_ID ORDER BY CONCAT(TIME,FLAG) DESC) = 0 THEN TIME ELSE '0' END AS BASE_TIME,
        | CASE WHEN SUM(FLAG) OVER (PARTITION BY TERMINAL_ID ORDER BY CONCAT(TIME,FLAG) DESC) = 0 THEN ERROR_CODE ELSE '0' END AS BASE_CODE
        |FROM
        | (
        |   SELECT TERMINAL_ID, ERROR_CODE, START_TIME TIME, 1 FLAG FROM T
        |     UNION ALL
        |   SELECT TERMINAL_ID, ERROR_CODE, END_TIME TIME, -1 FLAG FROM T
        |   ) T0
        |   ) T1
        |   ) T2 GROUP BY TERMINAL_ID, ERROR_CODE, START_TIME
        |""".stripMargin
    )
      .show(false)

  }

}


//val ds: Dataset[Row] = df.mapPartitions(iterator => {
//  val util = new Util()
//  val res = iterator.map{
//  case row=>{
//  val s: String = row.getString(0) + row.getString(1) + row.getString(2)
//  val hashKey: String = util.md5.digest(s.getBytes).map("%02X".format(_)).mkString
//  (hashKey, row.getInt(3)) }}
//  res
//  })