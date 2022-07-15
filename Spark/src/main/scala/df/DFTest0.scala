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
    df.createOrReplaceTempView("t")
    spark.sql(
      """
        | select id, time, lead(time,1,time) over (partition by id order by concat(time,flag) desc) as lead_time,
        |   case when sum(flag) over (partition by id order by concat(time,flag) desc) = 0
        |   and lead(time,1,'pp') over (partition by id order by concat(time,flag) desc) != time then time else "0" end base_time
        | from
        | (select id, st time , 1 flag from t
        | union all
        | select id, et time, -1 flag from t) tmp
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