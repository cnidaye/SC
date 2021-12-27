package groupedRdd

import org.apache.spark.{SparkConf, SparkContext}

object test1 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("test1").setMaster("local[2]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val r1 = sc.makeRDD(1 to 6)

    val r2 = r1.groupBy(_ % 2)
    // 返回的本质任是 rdd 【int， it】

    println(r2.aggregate(0)((acc,it) => acc + it._2.sum, _ + _))

    println(r2.+("123"))

    // reduceByKey =>
    r2.map(
      x => {
        (x._1,x._2.sum)
      }
    ).foreach(println)

//    r2.map((a,b) => (a, b.sum)).foreach(println)
    r1.checkpoint()

  }



}
