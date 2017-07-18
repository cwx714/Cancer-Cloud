package com.chargerlink.hta.demo

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
/**
  * Created by Cuiwx on 2017/7/13/013.
  */
object UserBehaviorsAnalysis {

  case class UserLog(logID: String, userID: String, time: String, typed: String, location: String, consumed: Double)
  case class LogOnce(logID: String, userID: String, count: Long)
  case class ConsumOnce(logID: String, userID: String, consumed: Double)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("UserBehaviorsAnalysis")
      .master("local")
      //.config("spark.sql.warehouse.dir", "file:///G:/IMFBigDataSpark2016/IMFScalaWorkspace_spark200/Spark200/spark-warehouse")
      .getOrCreate()

    import spark.implicits._

//    val userLogPath = args(0)
//    val userInfoPath = args(1)
    val userLogjson = "file:/D:/Work/Code/analysis-server/hta-server/data/userLog.json"
    val userInfojson = "file:/D:/Work/Code/analysis-server/hta-server/data/userInfo.json"
    val userInfo = spark.read.format("json").json(userInfojson)
    val userLog = spark.read.format("json").json(userLogjson)
//    userLogjson.write.format("parquet").save("file:/D:/Work/Code/analysis-server/hta-server/data/logparquet")
//    userInfojson.write.format("parquet").save("file:/D:/Work/Code/analysis-server/hta-server/data/userparquet")
//    val userInfo = spark.read.format("parquet").parquet("file:/D:/Work/Code/analysis-server/hta-server/data/userparquet")
//    val userLog = spark.read.format("parquet").parquet("file:/D:/Work/Code/analysis-server/hta-server/data/logparquet")
    //统计今天访问次数最多的top5 例如 2016-11-1-00:00:00  ~2016-11-1-23:59:59
    userLog.show()
    userLog.printSchema()
    userInfo.show()
    userInfo.printSchema()

    val startTime = "2016-11-1 00:00:00"
    val endTime = "2016-11-10 22:52:52"

    userLog.filter("time >= '" + startTime + "' and time <= '" + endTime + "'")
      //   userLog.filter("$'time' >= '" + startTime + "' and $'time' <= '" + endTime +"'")
      .join(userInfo, userInfo("userID") === userLog("userID"))
      .groupBy(userInfo("userID"), userInfo("name"))
      .agg(count(userLog("logID")).alias("userlogCount"))
      .sort($"userlogCount".desc)
      .limit(5)
      .show()

    //作业：生成parquet方式的数据，自己实现时间函数

    //统计今天购买次数最多的  Top5
    userLog.filter("time >= '" + startTime + "' and time <= '" + endTime + "'")
      .join(userInfo, userInfo("userID") === userLog("userID"))
      .groupBy(userInfo("userID"), userInfo("name"))
      .agg(round(sum(userLog("consumed")), 2).alias("totalConsumed"))
      .sort($"totalConsumed".desc)
      .limit(5)
      .show

    //统计特定时间段里访问次数增多最多的TOP5用户，例如这一周比上一周增长最快的5位用户
    //实现思路：计算这周用户的访问次数，同时计算上周用户的访问次数，相减以后排名
    //思路2 join
    //("time >= '2016-10-24' and time <= '2016-10-30'")
    //val userLogDS = userLog.as[UserLog].filter("time >= '2016-10-24' and time <= '2016-10-30'")
    /*   val userLogDS=userLog.as[UserLog]
       userLogDS.show()
      userLogDS.printSchema()*/
    //time >= '2016-11-1 00:00:00' and time <= '2016-11-10 22:52:52'

    val userLogDS = userLog.as[UserLog].filter("time >= '" + startTime + "' and time <= '" + endTime + "'")
      .map(log => LogOnce(log.logID, log.userID, 1))
      // .union(userLog.as[UserLog].filter("time >= '2016-10-17' and time <= '2016-10-23'")
      .union(userLog.as[UserLog].filter("time >= '2016-10-1 00:00:00' and time <= '2016-11-10 22:52:52'")
      .map(log => LogOnce(log.logID, log.userID, -1)))

    userLogDS.join(userInfo, userLogDS("userID") === userInfo("userID"))
      .groupBy(userInfo("userID"), userInfo("name"))
      .agg(sum(userLogDS("count")).alias("viewCountIncreased"))
      .sort($"viewCountIncreased".desc)
      .limit(5)
      .show()

    userLogDS.show()
    userLogDS.printSchema()

    //////////////////
    //购买金额前5的用户
    val userLogConsumerDS = userLog.as[UserLog].filter("time >= '2016-10-24' and time <= '2016-10-30'")
      //  val userLogDS=userLog.as[UserLog].filter("time >= '2016-11-1 22:00:00' and time <= '2016-11-2 22:00:00'")
      .map(log => ConsumOnce(log.logID, log.userID, log.consumed))
      .union(userLog.as[UserLog].filter("time >= '2016-10-17' and time <= '2016-10-23'")
        .map(log => ConsumOnce(log.logID, log.userID, -log.consumed)))

    userLogConsumerDS.join(userInfo, userLogConsumerDS("userID") === userInfo("userID"))
      .groupBy(userInfo("userID"), userInfo("name"))
      .agg(round(sum(userLogConsumerDS("consumed")), 2).alias("viewconsumedIncreased"))
      .sort($"viewconsumedIncreased".desc)
      .limit(5)
      .show()

    ///例如注册之后前10天访问我们的移动App最多的前五个人, userinfo加一个 字段 registeredTime
    userLog.join(userInfo, userLog("userID") === userInfo("userID"))
      .filter(userInfo("registeredTime") >= "2016-11-1"
        && userInfo("registeredTime") <= "2016-11-20"
        && userLog("time") >= userInfo("registeredTime")
        && userLog("time") <= date_add(userInfo("registeredTime"), 10))
      .groupBy(userInfo("userID"), userInfo("name"))
      .agg(round(count(userLog("consumed")),2).alias("totalconsumed"))
      .sort($"totalconsumed".desc)
      .limit(5)
      .show()

    ///或者注册之后前10天内购买商品总额排名前5为的人
    userLog.join(userInfo, userLog("userID") === userInfo("userID"))
      .filter(userInfo("registeredTime") >= "2016-11-1"
        && userInfo("registeredTime") <= "2016-11-20"
        && userLog("time") >= userInfo("registeredTime")
        && userLog("time") <= date_add(userInfo("registeredTime"), 10))
      .groupBy(userInfo("userID"), userInfo("name"))
      .agg(sum(userLog("logID")).alias("logTimes"))
      .sort($"logTimes".desc)
      .limit(5)
      .show()
  }
}
