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

    val DataLines = getIrisDataLines(sc)
    println(DataLines.count())
    println(DataLines)


    val b = sc.textFile("a.csv")
    println(b.count())
    println(b)

    log.info("finished")
    println("================================================")
  }
  private def getIrisDataLines(sc: SparkContext): RDD[String] = {
      val irisDataContents = Source.fromURL(
      getClass.getResource("/a.csv"))
      .getLines.takeWhile(_ != "").toSeq
      sc.parallelize(irisDataContents)
  }
}
