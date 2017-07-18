//package com.chargerlink.hta.demo
//
///**
//  * Created by Cuiwx on 2017/7/11/011.
//  */
//import org.apache.spark.SparkConf
//import org.apache.spark.sql.types.{StringType, StructField, StructType}
//import org.apache.spark.sql.{Row, SparkSession}
//
//object EBUserAnalyzerDataSet {
//  def main(args: Array[String]): Unit = {
//    var masterUrl = "local[*]"
//    var dataPath = "data/movielens/medium/"
//    if (args.length > 0) {
//      masterUrl = args(0)
//    } else if (args.length > 1) {
//      dataPath = args(1)
//    }
//    val sparkConf = new SparkConf().setMaster(masterUrl).setAppName("EBUserAnalyzerDataSet")
//
//    val spark = SparkSession
//      .builder()
//      .config(sparkConf)
//      .getOrCreate()
//    // For implicit conversions like converting RDDs to DataFrames
//
//    val sc = spark.sparkContext
//    sc.setLogLevel("WARN")
//
//    val userInfo=spark.read.format("parquet").parquet("parquet file's path ...")
//    val userAccessLog=spark.read.format("parquet").parquet("parquet file's path ...")
//
//    val userInfo=spark.read.format("json").json("json file's path ...")
//    val userAccessLog=spark.read.format("json").json("json file's path ...")
//
//    val userInfo=spark.read.json("json file's path ...")
//    val userAccessLog=spark.read.json("json file's path ...")
//
//    //检查数据
//    usersInfo.select("time").show()
//    usersInfo.show()
//    //检查schema
//    userInfo.printSchema()
//    userAccessLog.printSchema()
//
//    userAccessLog.filter("time >= 2017-1-1 and time <=2017-1-10")
//      .join(userInfo,userAccessLog("UserID")===userInfo("UserID"))
//      .groupby(usersInfo("UserID"),usersInfo("name"))
//      .agg(count(userAccessLog("time")).alias("userCount"))
//      .sort($"usercount".desc)
//      .limit(10)
//      .show()
//
//    //功能二：特定时段购买金额Top10
//    userAccessLog.filter("time >= 2017-1-1 and time <=2017-1-10")
//      .join(userInfo,userAccessLog("UserID")===userInfo("UserID"))
//      .groupby(usersInfo("UserID"),usersInfo("name"))
//      .agg(round(sum(userAccessLog("consumed")),2).alias("totalCount"))
//      .sort($"totalCount".desc)
//      .limit(10)
//      .show()
//
//    //功能3：访问次数增长Top10
//    case class UserLog(logID:Long,userID:Long,time:String,typed:Long,consumed:Double )
//    case class LogOnce(logID:Long,userID:Long,count:Long)
//
//    //Row转换为dataset
//    val userAccessTemp= userAccessLog.as[userLog].filter（"time >= 2017-1-1 and time <=2017-1-10 and typed =0")
//    .map(log =>LogOnce(log.logID,log.userID,1))
//      .union(userAccessLog.as[userLog].filter（"time >= 2017-1-20 and time <=2017-1-30 and typed =0"）
//    .map(log =>LogOnce(log.logID,log.userID,-1)))
//
//
//    // 这里有个小技巧，两个时间段分别统计，count计数一个为正数，一个为负数，然后通过agg进行求和，就能统计出
//    // 后一个时间段比前一个时间段访问次数的增长数
//
//    userAccessTemp.join(userInfo,userAccessTemp("userID")===usersInfo("userID"))
//      .groupBy("userID"),userInfo("name"))
//    .agg(sum(userAccessTemp("count")).alias("viewIncreasedTmp"))
//      .sort("userAccessTemp".desc)
//      .limit(10)
//      .show()
//
//    sc.stop()
//
//  }
//}
