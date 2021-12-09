package streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object test1 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[1]").setAppName("test")
    val ssc = new StreamingContext(conf, Seconds(2))
    val stream = ssc.socketTextStream("localhost",1234)
    stream.print()
    ssc.start()
    ssc.awaitTermination()
  }

}
