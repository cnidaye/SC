package rdd

import org.apache.spark.{SparkConf, SparkContext}

object test1 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("test1").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val r1 = sc.makeRDD(1 to 10)

    // 先合并分区内容再合并各分区结果
    r1.aggregate(0)((acc, a) => acc + a, (a,b) => a+b)
    //todo partition的数量和master有关
    r1.foreachPartition(x => println(x.sum))
  }

}
