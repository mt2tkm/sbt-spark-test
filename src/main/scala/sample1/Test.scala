package sbtsparktest.sample1

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

import scala.collection.mutable.HashMap
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
    val sql = "select _4,_6,_7,_8,_13,_14,_15,_18,_19,_20,_21,_22,sum(_10) as _10,sum(_11) as _11,sum(_12) as _12 from test group by _4,_6,_7,_8,_13,_14,_15,_18,_19,_20,_21,_22"
    val test_view = sqlContext.sql(sql)
    val name = Seq( "item", "aFlg", "bFlg", "device", "cFlg","sex", "age", "region", "color", "colorcate", "size", "bigcate", "addfee", "fee", "num" )
    val view = test_view.toDF(name: _*)

    //クエリ結果の作成？
    //SearchSD(view)
    SeeDB(view)

    log.info("finished")
    println("================================================")
  }

  //SeeDB
  private def SeeDB(df : DataFrame){
      val map_a = scala.collection.mutable.HashMap.empty[String, scala.collection.mutable.HashMap[String,(Int,Int,Int)]]
      val map_b = scala.collection.mutable.HashMap.empty[String, scala.collection.mutable.HashMap[String,(Int,Int,Int)]]
      val col = df.columns

      val A = (7,"大阪府")
      val B = (7,"神奈川県")

      //df.filter("region = '大阪府'").show
      df.rdd.filter{row => row(A._1).toString == A._2 || row(B._1).toString == B._2}
      .toLocalIterator.foreach{line =>
          val addfee = line(12).toString.toInt
          val fee = line(13).toString.toInt
          val num = line(14).toString.toInt

          if( line(A._1).toString == A._2 ){
          // 条件Aの内容を含むもの
              for(i<-0 until col.length-3){
                  if(i != A._1){
                      if(map_a contains col(i)){
                          if(map_a(col(i)) contains line(i).toString){
                              map_a(col(i))(line(i).toString) = ( map_a(col(i))(line(i).toString)._1 + addfee, map_a(col(i))(line(i).toString)._2 + fee, map_a(col(i))(line(i).toString)._3 + num)
                          }
                          else{
                              map_a(col(i)) += line(i).toString -> (addfee, fee, num)
                          }
                      }
                      else{
                          map_a(col(i)) = HashMap(line(i).toString -> (addfee, fee, num))
                      }
                  }
              }
          }
          else{
          // 条件Bの内容を含むもの
              for(i<-0 until col.length-3){
                  if(i != B._1){
                      if(map_b contains col(i)){
                          if(map_b(col(i)) contains line(i).toString){
                              map_b(col(i))(line(i).toString) = ( map_b(col(i))(line(i).toString)._1 + addfee, map_b(col(i))(line(i).toString)._2 + fee, map_b(col(i))(line(i).toString)._3 + num)
                          }
                          else{
                              map_b(col(i)) += line(i).toString -> (addfee, fee, num)
                          }
                      }
                      else{
                          map_b(col(i)) = HashMap(line(i).toString -> (addfee, fee, num))
                      }
                  }
              }
          }
      }
      for(i<-map_a.keys){
          println(i)
          println(map_a(i))
      }
      println("-------------------------------------------")
      for(i<-map_b.keys){
          println(i)
          println(map_b(i))
      }

  }

  //Search subset of data
  private def SearchSD(df : DataFrame){
      val map = scala.collection.mutable.HashMap.empty[String, scala.collection.mutable.HashMap[String,(Int,Int,Int)]]

      df.rdd.toLocalIterator.foreach{line =>
          val addfee = line(12).toString.toInt
          val fee = line(13).toString.toInt
          val num = line(14).toString.toInt

          //条件指定
          val x_value = line(7).toString
          val subsetdata = line(6).toString

          //All data
          if(map contains "all"){
              if(map("all") contains x_value){
                  map("all")(x_value) = ( map("all")(x_value)._1 + addfee, map("all")(x_value)._2 + fee, map("all")(x_value)._3 + num )
              }
              else{
                  map("all") +=  x_value -> (addfee, fee, num)
              }
          }
          else{
              map("all") = HashMap(x_value -> (addfee, fee, num))
          }

          //subset data
          if(map contains subsetdata){
              if(map(subsetdata) contains x_value){
                  map(subsetdata)(x_value) = ( map(subsetdata)(x_value)._1 + addfee, map(subsetdata)(x_value)._2 + fee, map(subsetdata)(x_value)._3 + num)
              }
              else{
                  map(subsetdata) += x_value -> (addfee, fee, num)
              }
          }
          else{
              map(subsetdata) = HashMap(x_value -> (addfee, fee, num))
          }
      }
      for(i<-map.keys){
          println(i)
          println(map(i))
      }
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
