package df
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}


object DFCreator {


  def main(args: Array[String]): Unit = {


    val spark = SparkSession.builder().appName("test").master("local[2]").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._

    val path = "D:\\MyProject\\SC\\Spark\\src\\main\\scala\\df\\data.csv"

    val rdd = sc.makeRDD(1 to 10)
    val data = 1 to 10

    val structureType = new StructType(
      Array(
       StructField("name",StringType),
        StructField("age2", IntegerType)
      )
    )

//    rdd.toDS().printSchema()
//
//    data.toDS().printSchema()
//
//    rdd.map(x=>Age(x)).toDS()

    spark.sparkContext.textFile(path)
      .map(_.split(","))
      .toDS()
      .printSchema()

    val csv = spark.read.format("csv")
      .option("header","true")
//      .option("inferSchema","true")
      .option("mode","FAILFAST")
      .schema(structureType)
      .load(path)
    csv.show()


  }

}
case class Age(myAge:Int)
case class Stu(name:String, age:Int)