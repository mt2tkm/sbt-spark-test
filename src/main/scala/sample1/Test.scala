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
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    println("================================================")

    //データを読み込み、データフレーム化
    val DataRDD =  getData(sc)
    val testDF = DataRDD.toDF

    //仮想的なviewの作成
    testDF.registerTempTable("test")
    val sql = "select _4,_5,_6,_7,_8,_13,_14,_15,_18,_19,_20,_21,_22,sum(_10),sum(_11),sum(_12) from test group by _4,_5,_6,_7,_8,_13,_14,_15,_18,_19,_20,_21,_22"
    val test_view = sqlContext.sql(sql)

    //クエリ結果の作成


    log.info("finished")
    println("================================================")
  }

  //brand,bigcate,smallcateがうまくいかない
  private def getData(sc:SparkContext): RDD[(Int,Int,Int,Int,Int,Int,Int,Int,String,Int,Int,Int,Int,Int,String,String,String,String,String,String,String,Int)] =
      sc.textFile("./src/main/resources/sample.csv").map{lines=>
          val elms = lines.split(",")

          val id = elms(0).toInt
          val orderid = elms(1).toInt
          val orderdetail = elms(2).toInt
          val item = elms(3).toInt
          val itemdetail = elms(4).toInt
          val aFLG = elms(5).toInt
          val bFLG = elms(6).toInt
          val device = elms(7).toInt
          val date = elms(8)
          val addFee = elms(9).toInt
          val fee = elms(10).toInt
          val num = elms(11).toInt
          val cFLG = elms(12).toInt
          val sex = elms(13).toInt
          val age = elms(14)
          val register = elms(15)
          val withdrawal = elms(16)
          val region = elms(17)
          val color = elms(18)
          val colorcate = elms(19)
          val size = elms(20)
          val bigcate = elms(21)
          val smallcate = elms(22)
          val shop = elms(23).toInt
          val brand = elms(24)

          (id, orderid, orderdetail, item, itemdetail, aFLG, bFLG, device, date, addFee, fee, num, cFLG, sex, age, register, withdrawal, region, color, colorcate, size, shop)
      }

}
