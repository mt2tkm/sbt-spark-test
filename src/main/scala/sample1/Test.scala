package sbtsparktest.sample1

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

import org.slf4j.LoggerFactory
import scala.io.Source

object test {
  def main(args: Array[String]) {
    val log = LoggerFactory.getLogger(getClass)
    val conf = new SparkConf().setAppName("Test").setMaster("local[*]")
    val sc = new SparkContext(conf)
    println("================================================")

    log.info("loading data")
    /*
    val DataLines = getIrisDataLines(sc)
    println(DataLines.count())
    println(DataLines)
    */

    val b = sc.textFile("./src/main/resources/b.csv")

    val ma = scala.collection.mutable.HashMap.empty[String,Int]

    for(data<-b.map(_.split(","))){
        ma(data(1)) = data(0).toInt
        println(ma)
    }
    println("forの外")
    println(ma)






    log.info("finished")
    println("================================================")
  }
  private def getIrisDataLines(sc: SparkContext): RDD[String] = {
      val irisDataContents = Source.fromURL(
      getClass.getResource("/b.csv"))
      .getLines.takeWhile(_ != "").toSeq
      sc.parallelize(irisDataContents)
  }
}
