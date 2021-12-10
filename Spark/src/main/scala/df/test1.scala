package df

import scala.collection.mutable.ArrayBuffer


object test1 {
  def main(args: Array[String]): Unit = {
    val spark = org.apache.spark.sql.SparkSession.builder().appName("test1").master("local[2]").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    import spark.implicits._
    val src = spark.read
      .option("header","true")
      .option("delimiter","\t")
      .csv("D:\\MyProject\\SC\\Data\\test.csv")
//      .as[fields]

    val mapping = Seq("code1","code2","code3")


//    src.show()

    src.rdd.map(x => Fields(x.getString(0),x.getString(1),x.getString(2)))
      .map(
        x => (
          x.id,
          x.name,
          {
            val ix = convertor(x.data)
            val s = ArrayBuffer[String]()
            for (i <- ix){
              s.append(mapping(i))
            }
            println(s)
            s.mkString(",")
          }
        )
      )
      .foreach(println)


  }

  def convertor(s:String) = {
    val list = s.split(",")
    val t = ArrayBuffer[Int]()
    for( i <- 0 until list.size)
      {
        if(list(i) equals "false") {
          t.append(i)
        }
      }
    t
  }

  case class Fields(id:String,name:String,data:String)
}
