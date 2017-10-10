package xuchao

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 用户资产匹配等待天数中间表
  * Created by xuchao on 2017-2-22.
  */
object UserAssetMatchedWait extends App {

  val config = new SparkConf()

  val sc = new SparkContext(config)

  val hiveContext = new HiveContext(sc)

  var beginDate = "2016-12-31"

  val tableName = "user_asset_matched_wait" //存入表表名

  import hiveContext.implicits._

  import Array._

  //获取表中最大日期 如果日期为空则使用默认起始日期
  val maxTime = hiveContext.sql("select max(log_create_date) from jlc_asset.user_asset_matched_wait").toDF().head().getString(0)

  if (maxTime != null)
    beginDate = maxTime

  //日期格式化
  val dateFormat = new SimpleDateFormat("yyyy-MM-dd")

  var endDate = dateFormat.format(new Date())

  //传入参数用来恢复数据使用
  if (this.args.length == 1)
    endDate = this.args(0)
  else if (this.args.length == 2){
    beginDate = this.args(0)
    endDate = this.args(1)
  }

  //数字格式化 - 保留两位小数
  def numberFormat (value : Double) : Double = "%.2f".format(value).toDouble

  val _assetData = hiveContext.sql("select user_id,user_type,matched_ratio,matched_amount,unouted_unmatched_amount,outed_unmatched_amount,date_format(log_create_date,'yyyy-MM-dd') as log_create_date " +
    "from jlc_asset.ast_user_match_priority where log_create_date > '" + beginDate + "' and log_create_date < '" + endDate + "' and (matched_amount + outed_unmatched_amount + unouted_unmatched_amount) > 100 order by user_id,log_create_date").toDF()

  if(_assetData == null){
    println("****** 没有查询到数据 ******")
    sc.stop()
  }

  //输出类型
  case class User_asset_match_transition(userId : String,user_type : Int,start_date : String,start_ratio : Double,end_date : String,end_ratio : Double,premium : Double)
  case class User_asset_match_temp(userId : String,user_type : Int,matched_ratio : Double,matched_amount : Double,unouted_unmatched_amount : Double,outed_unmatched_amount : Double,log_create_date : String,premium : Double, premium_diff : Double, remarks : String)

  case class Temp(userId : String, matched_ratio : Double, log_create_date : String)

  //添加存量列
  val addPremiumColumn = udf((matched_amount : Double,unouted_unmatched_amount: Double, outed_unmatched_amount : Double) => {
    val premiumValue : Double = matched_amount + unouted_unmatched_amount + outed_unmatched_amount
    premiumValue
  })

  //添加存量列
  val userAssetData = _assetData.withColumn("premium",addPremiumColumn($"matched_amount",$"unouted_unmatched_amount",$"outed_unmatched_amount"))

  val test = userAssetData.rdd.groupBy(row => row.getString(0)).flatMap{row =>
    user_match_method(row._2.toList)
  }

  val finalResult = hiveContext.createDataFrame(test)

  finalResult.show()
  finalResult.registerTempTable("tempTable")
  finalResult.sqlContext.sql("insert into table jlc_asset." + tableName + " partition(log_date='" + endDate + "') " +
    "select userId,matched_ratio,log_create_date from tempTable")

  println("****** UserAssetMatchedWait 完成 ******")
  sc.stop()

  /*
    * 中位数估算
    * @param m1
    * @param m2
    * @param s1
    * @param s2
    * @param alpha
    * @param beta
    * @param remark
    * @return
    */
  def median_estimate (m1 : Double,m2 : Double, s1 : Double, s2 : Double,alpha : Double, beta : Double, remark : String) : Double = {
    var ratio : Double = 0
    var alphaTemp : Double = 0
    var betaTemp : Double = 0.1
    if (remark == "N/A")
      ratio

    if(alpha == 0)
      alphaTemp = s1/(s1+s2)
    if(beta != 0)
      betaTemp = beta

    if(remark == "invest"){
      if (m2<m1)
        ratio = m1/s1
      else
        ratio = ((m1 + (alphaTemp*(m2-m1)))/s1)
    }
    if (remark == "redeem"){
      if(m2+betaTemp*(s1-s2)-m1<0)
        ratio = m1/s1
      else
        ratio = ((m1 + alphaTemp*(m2 - m1 + betaTemp*(s1-s2)))/s1)
    }

    ratio
  }

  def user_match_method (rows: List[Row]) = {

    var arr1 : Array[User_asset_match_temp] = addPremiumDiffColumn(rows)

    var result = Array[Temp]()

    var tempRatio : Double = 0
    var trueRatio : Double = 0
    var temp : Temp = null
    for(i <- 0 until arr1.length){
      if (i!=arr1.length-1){
        tempRatio = median_estimate(arr1(i+1).matched_amount, arr1(i).matched_amount, arr1(i+1).premium, arr1(i).premium, 0, 0, arr1(i).remarks)
        if (arr1(i).matched_ratio>tempRatio)
          trueRatio = arr1(i).matched_ratio
        else
          trueRatio = tempRatio
        temp = new Temp(arr1(i).userId, trueRatio,arr1(i).log_create_date)
        result = concat(result, Array[Temp](temp))
      }
      /*else
        temp = new Temp(arr1(i).userId, arr1(i).matched_ratio, arr1(i).log_create_date)*/

    }

    result
  }

  /*
    * 添加存量差列
    */
  def addPremiumDiffColumn (rows : List[Row]) : Array[User_asset_match_temp] = {
    var userInfo = Array[User_asset_match_temp]()

    /*
      * 遍历记录计算存量差值
      * 如果是第一条记录则设置为0
      * 否则为后一天存量-前一天的存量
      *
      * 添加完存量差后添加remark列
      * 如果 存量差 >= 100 标记为 invest
      * 如果 存量差 <= -100 标记为 redeem
      * 否则标记为 N/A
      *
      */
    for (i <- 0 until rows.size){
        var premiumDiff : Double = 0
        val row = rows.apply(i)
        if (i == 0) premiumDiff = 0
        else
          premiumDiff = numberFormat(row.getDouble(7) - rows.apply(i-1).getDouble(7))
        var remarks = ""
        if (premiumDiff >= 100) remarks = "invest" else if (premiumDiff <= -100) remarks = "redeem" else remarks = "N/A"
        val user = new User_asset_match_temp(row.getString(0),row.getInt(1),numberFormat(row.getDouble(2))*100,row.getDouble(3),row.getDouble(4),row.getDouble(5),row.getString(6),row.getDouble(7),premiumDiff,remarks)
        userInfo = concat(userInfo, Array[User_asset_match_temp](user))
      }
    userInfo
  }



}
