package rdd

import org.apache.spark.{SparkConf, SparkContext}

object test1 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("test1").setMaster("local[2]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val r1 = sc.makeRDD(1 to 6)

    // 先合并分区内容再合并各分区结果
    println(r1.aggregate(0)((acc, a) => acc + a, (a, b) => a + b))
    //partition的数量和master core设置有关
    r1.foreachPartition(x => println(x.sum))
    //
    println(r1.reduce((a, b) => a + b))

    val r2 = r1.map(_ + 1)
    //    r1.cartesian(r2).foreach(println)

    //glom


    r1.map(x => (x,1))

    r1.zip(sc.makeRDD(Array("a","b","c","d","e","f"))).foreach(x => println(x._1,x._2))

  }
}
