package rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object test03 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("123").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val rdd = sc.textFile("D:\\MyProject\\SC\\Data\\wc.txt")

    val res = withStopWordsFiltered(rdd, Array('\t'), null)

    res.foreach(println)


  }

  def withStopWordsFiltered(rdd : RDD[String], illegalTokens : Array[Char],
                            stopWords : Set[String]): RDD[(String, Int)] = {
    val separators = illegalTokens ++ Array[Char](' ')
    val tokens: RDD[String] = rdd.flatMap(_.split(separators).
      map(_.trim.toLowerCase))
    val words = tokens.filter(token =>
      !stopWords.contains(token) && (token.length > 0) )
    val wordPairs = words.map((_, 1))
    val wordCounts = wordPairs.reduceByKey(_ + _)
    wordCounts
  }
}
