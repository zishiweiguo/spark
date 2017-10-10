package xuchao

import java.text.SimpleDateFormat
import java.util.{Date, PriorityQueue}

import org.apache.commons.math3.distribution.NormalDistribution
import org.apache.commons.math3.stat.inference.{AlternativeHypothesis, BinomialTest}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime

import scala.Array._
import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks

/**
  * 用户工资计划投资潜力挖掘
  * Created by xuchao on 2017-9-13.
  */
object Wage_invest_mode_recognition {

  case class ResultBean(userId : String, startDay : Int, endDay : Int, amount : Double, concentrateConf:Double, stableConf:Double, totalScore: Double)

  /**
    * 辅助函数：识别连续数字并分组；返回分组信息
    */
  def findGroup(x: ArrayBuffer[Double]): Array[Int] = {
    val size = x.length
    val index = new Array[Int](size)
    var r = 0
    var before = x.apply(0)
    for (i <- 1 until size) {
      val now = x.apply(i)
      if ((now - before) == 1) {
        index(i) = r
      } else {
        r += 1
        index(i) = r
      }
      before = now
    }
    index
  }

  // 日期转换  年月日 -> 年月日 时分秒
  def conversionDate(dateStr: String): String = new DateTime(new SimpleDateFormat("yyyy-MM-dd").parse(dateStr).getTime).toString("yyyy-MM-dd HH:mm:ss")

  /**
    * 日期截取
    * yyyy-MM-dd HH:mm:ss  中截取 年、月、日、时、分、秒
    *
    * @param dateStr
    * @param pattern
    * @return
    */
  def udfSplitDate(dateStr: String, pattern: String): String = new DateTime(
    new SimpleDateFormat("yyyy-MM-dd").parse(dateStr).getTime).toString(pattern)

  /**
    * 找中位数
    *
    * @param array
    * @return
    */
  def median(array: ArrayBuffer[Double]): Double = {
    val heapSize = array.length / 2 + 1
    val heap = new PriorityQueue[Double](heapSize)
    for (i <- 0 until heapSize) {
      heap.add(array.apply(i))
    }
    for (i <- heapSize until array.length) {
      if (heap.peek() < array.apply(i)) {
        heap.poll()
        heap.add(array.apply(i))
      }
    }
    var r = 0.0
    if (array.length % 2 == 1)
      r = heap.peek()
    else
      r = (heap.poll() + heap.peek()) / 2.0
    r
  }

  /**
    * 求方差
    *
    * @param a
    * @return
    */
  def variance(a: ArrayBuffer[Double]): Double = {
    var variance = 0.0 //方差
    var sum = 0.0
    var sum2 = 0.0
    val len = a.length
    for (i <- 0 until len) {
      sum += a.apply(i)
      sum2 += Math.pow(a.apply(i), 2)
    }
    variance = sum2 / len - (sum / len) * (sum / len)
    variance
  }

  /**
    * 对给定时间区间进行时间集中度检验
    * x     给定时间区间
    * arr1  原始数据集
    * arr2  月份补全数据集
    * arr3  权重数组
    * N
    * p
    */
  def timeConcentrate(x: Int, arr1: Iterable[Array[String]], arr2: Iterable[Array[String]], arr3: Array[Double], N: Int, p: Double): Double = {
    val validArr = arr1.filter { v => if ((udfSplitDate(v.apply(1), "dd").toInt >= x) && (udfSplitDate(v.apply(1), "dd").toInt <= (x + 4))) true else false }
    val tempArr = new Array[Double](arr2.size)
    var count = 0
    for (a <- arr2) {
      var flag = 0
      Breaks.breakable {
        for (b <- validArr) {
          if (a.apply(2).equals((udfSplitDate(b.apply(1), "yyyy").toInt * 100 + udfSplitDate(b.apply(1), "MM").toInt).toString)) {
            flag = 1
            Breaks.break()
          }
        }
      }
      tempArr(count) = flag * arr3.apply(count)
      count += 1
    }
    val judge = tempArr.sum
    val btest = new BinomialTest()
    val v = 1 - btest.binomialTest(N, Math.floor(judge).toInt, p, AlternativeHypothesis.GREATER_THAN)
    v
  }

  /**
    * 金额集中度的假设检验
    * 输出金额时间集中度的可信度、平均金额和金额波动性评价分数
    */
  def moneyConcentrate(x: Int, arr1: Iterable[Array[String]], arr2: Iterable[Array[String]],
                       r: Array[Double], N: Int, trialsInMonth: Int,
                       meanSuccess: Double, sdSuccess: Double, confLevel: Double): Array[Double] = {
    //在未处理的数据集中过滤出day>=x 并且<= x+4
    val focusArr = arr1.filter { v => if ((udfSplitDate(v.apply(1), "dd").toInt >= x) && (udfSplitDate(v.apply(1), "dd").toInt <= (x + 4))) true else false }
      .map(row => (row.apply(0), row.apply(1), row.apply(2), (udfSplitDate(row.apply(1), "yyyy").toInt * 100 + udfSplitDate(row.apply(1), "MM").toInt).toString)).groupBy(row => row._4)
      .map(k => (k._2.map(i => i._1).head, k._2.map(i => i._3.toDouble).sum.toString, k._2.map(i => i._4).head))
    var resultArr = new ArrayBuffer[Array[String]]()
    for (a <- arr2) {
      var f = false
      Breaks.breakable {
        for (b <- focusArr) {
          if (a.apply(2).equals(b._3)) {
            f = true
            resultArr += Array(b._1, b._2, b._3, a.apply(1))
            Breaks.break()
          }
        }
      }
      if (!f)
        resultArr += Array(a.apply(0), "0", a.apply(2), a.apply(1))
    }

    val normalDistribution = new NormalDistribution()
    val resultConfArr = resultArr.map(row =>
      (row.apply(0), row.apply(1), row.apply(2), row.apply(3), normalDistribution.cumulativeProbability((row.apply(1).toDouble * trialsInMonth / (row.apply(3).toDouble + 0.01) - meanSuccess) / sdSuccess))
    )

    val tempArr = new Array[Double](resultConfArr.length)
    for (a <- resultConfArr.indices) {
      tempArr(a) = resultConfArr.apply(a)._5 * r.apply(a)
    }

    val judge = tempArr.sum
    val concentrateConf = normalDistribution.cumulativeProbability((judge - 0.5 * N) / Math.pow(N.toFloat / 12.toFloat, 0.5).formatted("%.4f").toDouble) * 100

    val selectAmountArr = resultConfArr.filter { r => if (r._5 >= confLevel) true else false }
    val selectAmount = selectAmountArr.map(row => row._2.toDouble)
    var stableConf = 0.0
    var meanAmount = 0.0
    if (selectAmountArr.length >= 3) {
      meanAmount = median(selectAmount)
      stableConf = (120 / (1 + Math.exp(8.047 * ((Math.pow(variance(selectAmount), 0.5) / (selectAmount.sum / selectAmount.length)) - 0.2)))).formatted("%.2f").toDouble
    }
    val result = new Array[Double](4)
    result(0) = x
    result(1) = concentrateConf.toDouble
    result(2) = meanAmount
    result(3) = stableConf
    result
  }

  def algorithmHandle(row: (String, Iterable[Array[String]])) = {
    var result = new ArrayBuffer[Array[String]]()
    val userInvestLog = row._2
    val userInvestMonthSummary = userInvestLog.map(row => (row.apply(0), row.apply(1), row.apply(2), (udfSplitDate(row.apply(1), "yyyy").toInt * 100 + udfSplitDate(row.apply(1), "MM").toInt).toString))
      .groupBy(row => (row._1, row._4)).map(k => (k._2.map(i => i._1).head, k._2.map(i => i._3.toDouble).sum.toString, k._2.map(i => i._4).head))

    val minMonth = userInvestMonthSummary.minBy(c => c._3)._3.toInt
    val maxMonth = userInvestMonthSummary.maxBy(c => c._3)._3.toInt

    val arr1 = Array(2015 to 2017)
    val arr2 = Array(1 to 12)
    var resultArr = new ArrayBuffer[Array[String]]()
    for (x <- arr1.apply(0)) {
      for (y <- arr2.apply(0)) {
        if (y != 1 && y != 2 && y != 10) {
          val z = x * 100 + y
          if (minMonth <= z && z <= maxMonth) {
            var f = false
            Breaks.breakable {
              for (u <- userInvestMonthSummary) {
                if (u._3.toInt == z) {
                  f = true
                  resultArr += Array(u._1, u._2, u._3)
                  Breaks.break()
                }
              }
            }
            if (!f)
              resultArr += Array(row._1, "0", z.toString)
          }
        }
      }
    }

    val daySpan = 5
    val conf_level = 0.97
    val p = daySpan.toDouble / 30
    val alpha = 0.7368063
    val trialsInMonth = 30
    val meanSuccess = trialsInMonth * p
    val sdSuccess = trialsInMonth * p * Math.pow((1 - p), 0.5)

    val N = resultArr.size

    val resultArrIndex = Array(0 until N)
    val rseq = new Array[Double](N)
    val arrs = new Array[Double](N)
    for (i <- resultArrIndex.apply(0).indices) {
      rseq(i) = Math.pow(alpha, resultArrIndex.apply(0).apply(i))
    }
    val rseqSum = rseq.sum
    for (i <- resultArrIndex.apply(0).indices) {
      arrs(i) = rseq.apply(N - 1 - i) * N / rseqSum
    }
    //arrs  倒序
    //生成一系列待检验的时间区间
    val day = userInvestLog.map(row => udfSplitDate(row.apply(1), "dd").toInt)
    val minDay = day.min // 日期中日最小的值
    val maxDay = day.max // 日期中日最大的值
    val end = if (minDay > maxDay - (daySpan - 1)) minDay else maxDay - (daySpan - 1)
    val timeLineStart = Array(minDay to end).apply(0) // day 数组

    // 检验1： 对这些时间区间进行时间集中度检验
    val timeFocusJudge = new Array[Double](timeLineStart.size)
    for (i <- timeLineStart.indices) {
      val v = timeConcentrate(timeLineStart.apply(i), userInvestLog, resultArr, arrs, N, p)
      timeFocusJudge(i) = v
    }

    /*
     * 通过时间集中度检验的时间区间
     * 过滤出timeFocusJudge 中值>= conf_level 对应的时间区间
     */
    val validTimeLineStart = new ArrayBuffer[Int]()
    for (i <- timeLineStart.indices) {
      if (timeFocusJudge.apply(i).formatted("%.2f").toDouble >= conf_level)
        validTimeLineStart += timeLineStart.apply(i)
    }

    //如果通过时间集中度检验的时间区间为空，则进入下一个循环
    if (validTimeLineStart.nonEmpty) {
      val tmp = new ArrayBuffer[Array[Double]]()
      //检验2： 进行金额集中度检验
      for (i <- validTimeLineStart.indices)
        tmp += moneyConcentrate(validTimeLineStart.apply(i), userInvestLog, resultArr, arrs, N, trialsInMonth, meanSuccess, sdSuccess, conf_level)

      val tmpArr = tmp.filter { r =>
        if (r.apply(3) >= 70 && r.apply(1) >= 85)
          true
        else
          false
      }

      if (tmpArr.nonEmpty) {
        val startArr = findGroup(tmpArr.map(r => r.apply(0)))
        //补全字段
        // userid , totalscore, label , end
        val fullTmp = new ArrayBuffer[Array[String]]()
        for (i <- tmpArr.indices) {
          val arr = new Array[String](8)
          arr(0) = row._1    // userid
          arr(1) = (tmpArr.apply(i).apply(1) + tmpArr.apply(i).apply(3)).toString  //totalscore
          arr(2) = startArr.apply(i).toString  // label
          arr(3) = tmpArr.apply(i).apply(0).toString //start
          arr(4) = tmpArr.apply(i).apply(1).toString  // concentrateConf
          arr(5) = tmpArr.apply(i).apply(2).toString  // meanAmount
          arr(6) = tmpArr.apply(i).apply(3).toString // stableConf
          fullTmp += arr
        }
        result = fullTmp
      }

    }

    var r = Array[ResultBean]()
    result.groupBy(row => row.apply(2)).map(row => row._2.maxBy(r => r.apply(1))).foreach{row =>
      r = concat(r,Array[ResultBean](new ResultBean(row.apply(0).toString, row.apply(3).toDouble.toInt, row.apply(3).toDouble.toInt + 4 , row.apply(5).toDouble, row.apply(4).toDouble, row.apply(6).toDouble, row.apply(1).toDouble)))
    }
    r
  }

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("invest_potential")

    val sc = new SparkContext(conf)
    val hiveContext = new HiveContext(sc)

    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    println("Job start : " + sdf.format(new Date()))

    //数据样本
//    var path = "E:/yinker/python/input.csv"
    /*
     * 1.获取数据集
     */
    /*val sampleRDD = sc.textFile(path)
    val initialRDD = sampleRDD.map(line => line.split(","))
    val header = initialRDD.first()

    initialRDD.filter(line => line.apply(0) != header.apply(0))
      .map(line => Array(line.apply(0), line.apply(1), line.apply(2)))
      .sortBy(x => (x.apply(0), x.apply(1))).groupBy(row => row.apply(0))
      .foreach { row =>
      println("executor algorighmHandle() 当前userId " + row._1 + "  =====> " + sdf.format(new Date()))
      algorithmHandle(row)
      println("finished algorighmHandle() =====> " + sdf.format(new Date()))
    }*/

    /*select c.userid,to_date(c.create_time) as `date`,sum(c.amount) as invest_amount " +
    "FROM (SELECT a.userid, a.create_time, a.amount, b.userid as id from fact_jlc.invest_record a LEFT JOIN (select distinct(userid) userid from dim_kylin.olap_user_portrait_history where mean_premium=\"brush\" and part_log_day>='2017-01-01') b " +
      "ON a.userid = b.userid where to_date(a.create_time)>='2017-01-01' and to_date(a.create_time)<='"+ udfSplitDate(sdf.format(new Date()), "yyyy-MM-dd") +"' ) c WHERE c.id is NULL group by c.userid,to_date(c.create_time)"*/

    val test = hiveContext.sql(args(0)).map{ line =>
        val r = new Array[String](3)
        r(0) = line.apply(0).toString
        r(1) = line.apply(1).toString
        r(2) = line.apply(2).toString
        r
      }.sortBy(x => (x.apply(0), x.apply(1))).groupBy(row => row.apply(0).toString)
      .flatMap{ row =>
        //        println("executor algorighmHandle() 当前userId " + row._1 + "  =====> " + sdf.format(new Date()))
        algorithmHandle(row)
      }
    val finalResult = hiveContext.createDataFrame(test)
    finalResult.show()
    finalResult.registerTempTable("finalresult")
    finalResult.sqlContext.sql("insert overwrite table dim_jlc.dim_invest_potential partition(part_log_day='"+ udfSplitDate(sdf.format(new Date()), "yyyy-MM") +"') " +
      "select userId,startDay,endDay,amount,concentrateConf,stableConf,totalScore from finalresult")

    println("Job stop time : " + sdf.format(new Date()))
    sc.stop()

  }

}
