package sbtsparktest.sample1

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

import org.apache.spark.sql._

import org.slf4j.LoggerFactory
import scala.io.Source

object test {
  def main(args: Array[String]) {
    //ログ出力用
    val log = LoggerFactory.getLogger(getClass)

    //SparkContext インスタンスの生成
    val conf = new SparkConf().setAppName("Test").setMaster("local[*]")
    val sc = new SparkContext(conf)

    //SQLContext インスタンスの生成

    println("================================================")
    log.info("loading data")
    //val Data = sc.textFile("./src/main/resources/sample.csv").map(_.split(","))
    val Data =  getData(sc).map(_.split(","))
    for(i<-Data){for(j<-i){println(j)}}
    print(Data)


    log.info("finished")
    println("================================================")
  }
  private def getData(sc: SparkContext): RDD[String] = {
      val DataContents = Source.fromURL(
      getClass.getResource("/sample.csv"))
      .getLines.takeWhile(_ != "").toSeq
      sc.parallelize(DataContents)
  }

}
