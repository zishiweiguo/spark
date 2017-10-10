package xuchao

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.Row
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.control.{Breaks}

/**
  * Created by xuchao on 2017-2-22.
  */
object UserAssetMatched extends App{

  val config = new SparkConf().setAppName("UserAssetMatched")

  val sc = new SparkContext(config)

  val hiveContext = new HiveContext(sc)

  if(this.args.length<4){
    throw new Exception("缺少参数 需要4个参数 ")
  }

  val ratioConf : Double = this.args(0).toDouble  //匹配率(0-100)
  val userType = this.args(1)   //用户类型 (1,2)
  val time = this.args(2)       //筛选时间
  val tableName = this.args(3)   //存入表表名

  var filterType = ""
  if (userType == "1" || userType == "2")
    filterType = " and user_type = " + userType

  import Array._ ,hiveContext.implicits._

  val _assetData = hiveContext.sql("select user_id,user_type,matched_ratio,matched_amount,unouted_unmatched_amount,outed_unmatched_amount,date_format(log_create_date,'yyyy-MM-dd') " +
    "from jlc_asset.ast_user_match_priority where log_create_date > '" + time + "'" + filterType + " and (matched_amount + outed_unmatched_amount + unouted_unmatched_amount) > 100 order by user_id,log_create_date").toDF()

  //输出类型
  case class User_asset_match_transition(userId : String,user_type : Int,start_date : String,start_ratio : Double,end_date : String,end_ratio : Double,premium : Double)
  case class User_asset_match_temp(userId : String,user_type : Int,matched_ratio : Double,matched_amount : Double,unouted_unmatched_amount : Double,outed_unmatched_amount : Double,log_create_date : String,premium : Double, premium_diff : Double, remarks : String)


  val start = new Date().getTime()

  //数字格式化 - 保留两位小数
  def numberFormat (value : Double) : Double = "%.2f".format(value).toDouble
  //日期格式化
//  val dateFormat1 = new SimpleDateFormat("yyyy-M-d")
  val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
//  val timeConvert = udf((time:String)=>dateFormat.format(dateFormat1.parse(time).getTime))
//  val assetData = _assetData.withColumn("log_create_date",timeConvert($"log_create_date"))

  //添加存量列
  val addPremiumColumn = udf((matched_amount : Double,unouted_unmatched_amount: Double, outed_unmatched_amount : Double) => {
    val premiumValue : Double = matched_amount + unouted_unmatched_amount + outed_unmatched_amount
    premiumValue
  })

  val userAssetData = _assetData.withColumn("premium",addPremiumColumn($"matched_amount",$"unouted_unmatched_amount",$"outed_unmatched_amount"))

//  val userAssetData = userAssetData0.withColumn("premium",$"premium")

  val test = userAssetData.rdd.groupBy(row => row.getString(0)).flatMap{row =>
    user_match_method(row._2.toList)
  }
  test.count()

  val end = new Date().getTime

  println("耗时" + (end-start).toString + "ms")

  val finalResult = hiveContext.createDataFrame(test)

  finalResult.show()
  finalResult.registerTempTable("finalresult")
  finalResult.sqlContext.sql("insert overwrite table jlc_asset." + tableName + " partition(log_date='"+dateFormat.format(new Date())+"') " +
    "select userId,user_type,start_date,start_ratio,end_date,end_ratio,premium from finalresult")

//  sf.write.mode(SaveMode.Overwrite).saveAsTable("jlc_asset.user_assets_match_transition")

  sc.stop()

  /**
    * 找出有效的匹配记录
    *
    * @return
    */
  def user_match_method (rows: List[Row]) = {

    /** *********************
      *      数据准备       *
      * *********************
      */
    //log_create_date 最小时间
    val min_log_create_date = rows.minBy(row => row.getString(6)).getString(6)

    //获取df中log_create_date最小值和‘2016-12-31’比较 如果大于该时间则用户是新用户，新用户需要在df前面添加一条空记录
    var arr1 : Array[User_asset_match_temp] = null
    if (min_log_create_date > "2016-12-31") arr1 = addPremiumDiffColumn(rows,min_log_create_date,0) else arr1 = addPremiumDiffColumn(rows,min_log_create_date,1)

    /*val df = sqlContext.createDataFrame(arr1)
    df.show(100)*/

    //在准备好的数据中找出remarks=invest的条数
    var investIdArr = Array[Int]()
    for(i<-0 until arr1.length){
      if (arr1(i).remarks == "invest") investIdArr = concat(investIdArr,Array[Int](i))
    }

    val userId = arr1(0).userId
    val user_type = arr1(0).user_type

    val loop = new Breaks()

    var resultTable = Array[User_asset_match_transition]()

    val investNum = investIdArr.size
    if(investNum == 0) resultTable
    else{
      for (i <- 0 until investNum) {
        val count = investIdArr(i) //起点下标
        var matched_amount : Double = 0
        if (count == 0)
          matched_amount = arr1(count).matched_amount
        else
          matched_amount = arr1(count-1).matched_amount
        val start_ratio = numberFormat(matched_amount * 100 / arr1(count).premium)   //起始匹配率
        val start_date = arr1(count).log_create_date
        var end_ratio : Double = 0
        var end_date : String = ""
        var premium : Double = 0
        var isEnd : Boolean = false
        var index : Int = 0 //终点下标

        loop.breakable {
          for(j <- count until arr1.length){
            index = j
            val remarks = arr1(j).remarks
            if (remarks == "invest" && i!=(investNum-1) && j == investIdArr(i+1)) { loop.break() }  //不是最后一条invest记录并且查找到下标等于下一条invest记录 即找到下一个invest记录时结束循环
            else{
              if (!isEnd) {
                //判断是否是redeem
                if (remarks == "redeem" && arr1(j).matched_ratio > arr1(j - 1).matched_ratio) {
                  end_ratio = numberFormat(arr1(j - 1).matched_ratio)
                  end_date = arr1(j - 1).log_create_date
                  premium = numberFormat(arr1(j - 1).premium)
                  isEnd = true
                  loop.break()
                }
                //判断匹配率是否>=80% 默认80
                if (arr1(j).matched_ratio >= ratioConf) {
                  end_ratio = numberFormat(arr1(j).matched_ratio)
                  end_date = arr1(j).log_create_date
                  premium = numberFormat(arr1(j).premium)
                  isEnd = true
                  loop.break()
                }
              }
            }
          }
        }
        if (!isEnd) {
          if (index == arr1.length - 1) {
            if (arr1(index).remarks == "invest") {
              end_ratio = numberFormat(arr1(index - 1).matched_ratio)
              end_date = arr1(index - 1).log_create_date
              premium = numberFormat(arr1(index - 1).premium)
            } else {
              end_ratio = numberFormat(arr1(index).matched_ratio)
              end_date = arr1(index).log_create_date
              premium = numberFormat(arr1(index).premium)
            }
          }else{
            end_ratio = numberFormat(arr1(index-1).matched_ratio)
            end_date = arr1(index-1).log_create_date
            premium = numberFormat(arr1(index-1).premium)
          }
        }
        //添加筛选出的数据放到Row中
        val resultRow  = new User_asset_match_transition(userId,user_type,start_date,start_ratio,end_date,end_ratio,premium)
        val tempArray = Array[User_asset_match_transition](resultRow)
        resultTable = concat(resultTable,tempArray)
      }
    }
    resultTable
  }

  /**
    * 添加存量差列
    * 如果index为1 则不计算第一条
    */
  def addPremiumDiffColumn (rows : List[Row], minDate : String, index : Int) : Array[User_asset_match_temp] = {
    var userInfo = Array[User_asset_match_temp]()

      for (i <- 0 until rows.size){
        val date = rows.apply(i).getString(6)
        var v : Double = 0
        if (date == minDate) if(index == 0) rows.apply(0).getDouble(7) else v
        else{
          for (i <- 1 until rows.size) {
            val dataString = rows.apply(i).getString(6)
            if (date == dataString) v = rows.apply(i-1).getDouble(7)
          }
          v = numberFormat(rows.apply(i).getDouble(7) - v)
        }
        var remarks = ""
        if (v >= 100) remarks = "invest" else if (v <= -100) remarks = "redeem" else remarks = "N/A"
        val row = rows.apply(i)
        val user = new User_asset_match_temp(row.getString(0),row.getInt(1),numberFormat(row.getDouble(2))*100,row.getDouble(3),row.getDouble(4),row.getDouble(5),row.getString(6),row.getDouble(7),v,remarks)
        userInfo = concat(userInfo, Array[User_asset_match_temp](user))
      }
    userInfo
  }



}
