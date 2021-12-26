package c1

import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object WC {

//  val flatMapper = new FlatMapFunction[String,String] {
//    override def flatMap(value: String, out: Collector[String]) = ???
//  }
  def main(args: Array[String]): Unit = {
   val env = StreamExecutionEnvironment.getExecutionEnvironment
  env.setParallelism(2)

  env.readTextFile("D:\\MyProject\\SC\\Data\\wc.txt")
    .flatMap(_.split(" "))
    .map( x => (x,1))
    .keyBy(_._1)
    .reduce((a,b) => (a._1, a._2+b._2))
    .print()

  env.readTextFile("")

  env.execute()

  }

}
