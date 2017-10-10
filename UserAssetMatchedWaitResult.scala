package xuchao

import java.text.SimpleDateFormat
import java.util.{Date, Calendar}

import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.control.Breaks

/**
  * 用户资产匹配等待时长
  * Created by xuchao on 2017-4-01.
  */
object UserAssetMatchedWaitResult extends App{

  val config = new SparkConf()

  val sc = new SparkContext(config)

  val hiveContext = new HiveContext(sc)

  //date formatting
  val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
  //value formatting - keep two decimal places
  def numberFormat (value : Double) : Double = "%.2f".format(value).toDouble

  //The day before the current date
  def preNDay(n: Int,date : String) : String = {
    val cal : Calendar = Calendar.getInstance()
    cal.setTime(dateFormat.parse(date))
    cal.add(Calendar.DATE, -n)
    val preDay = dateFormat.format(cal.getTime)
    preDay
  }

  var endDate = dateFormat.format(new Date())
  if (this.args.length==1)
    endDate = this.args(0)

  val pre1Day = preNDay(1,endDate)
  val pre20Day = preNDay(20,endDate)

  import Array._

  val userAssetData = hiveContext.sql("select userId,matched_ratio,log_create_date " +
    " from jlc_asset.user_asset_matched_wait where log_create_date>= '" + pre20Day + "' and log_create_date < '" + pre1Day + "' order by userId,log_create_date desc").toDF()

  if(userAssetData == null){
    println("****** jlc_asset.user_asset_matched_wait 没有查询到数据 ******")
    sc.stop()
  }

  //object type
  case class Temp(userId : String, matched_ratio : String, waitDay : Int)

  val resultRdd = userAssetData.rdd.groupBy(row => row.getString(0)).flatMap{row =>
    user_match_method(row._2.toList)
  }
  resultRdd.count()

  val finalResult = hiveContext.createDataFrame(resultRdd)

  finalResult.show()
  finalResult.registerTempTable("tempTable")

  finalResult.sqlContext.sql("insert into table jlc_asset.user_asset_matched_waitResult partition(log_date='"+endDate+"') " +
    "select '"+ pre1Day +"',a.ratio,COUNT(case when waitDay=1 then userId else null end)" +
    ",COUNT(case when waitDay=2 then userId else null end),COUNT(case when waitDay=3 then userId else null end)" +
    ",COUNT(case when waitDay=4 then userId else null end),COUNT(case when waitDay=5 then userId else null end)" +
    ",COUNT(case when waitDay=6 then userId else null end),COUNT(case when waitDay=7 then userId else null end) " +
    "from (select matched_ratio as ratio,userId,waitDay from tempTable ) a group by a.ratio ORDER BY a.ratio")


  sc.stop()


  /**
    * 根据匹配区间寻找等待天数
    * 匹配区间分为[0,10] [0,20] [0,30] [0,40] [0,50] [0,60] [0,70] [0,80] [0,90] [0,100]
    * wait days 1,2,3,4,5,6,7
    *
    * @param rows
    * @return
    */
  def user_match_method (rows: List[Row]) = {
    var result = Array[Temp]()
    for (j <- 1 until 11){
      var waitDay : Int = 0      //wait days
      val loop = new Breaks()
      loop.breakable {
        for (i <- 0 until rows.size){
          val ratio = rows(i).getDouble(1)
          if (0<=ratio && ratio<=j*10.toDouble){
            waitDay += 1
            if (waitDay == 7) loop.break()
          }else
            loop.break()
        }
      }
      if (waitDay != 0){  //等待天数为0的剔除
        val temp = new Temp(rows.head.getString(0),"[0," + j*10 + "]",waitDay)
        result = concat(result,Array[Temp](temp))
      }
    }

    result
  }

}
