package com.chargerlink.hta.demo

import org.apache.spark.sql.SparkSession

/**
  * Created by Cuiwx on 2017/7/11/011.
  */
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object MUAnalyzerDataFrame {
  def main(args: Array[String]): Unit = {
    var masterUrl = "local[*]"
    var dataPath = "file:/D:/Work/Code/analysis-server/hta-server/data/"
    if (args.length > 0) {
      masterUrl = args(0)
    } else if (args.length > 1) {
      dataPath = args(1)
    }
    val sparkConf = new SparkConf().setMaster(masterUrl).setAppName("MUAnalyzerDataFrame")

    val spark = SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate()

    import spark.implicits._
    // For implicit conversions like converting RDDs to DataFrames

    val sc = spark.sparkContext
    sc.setLogLevel("WARN")

    val usersRDD = sc.textFile(dataPath + "users.dat")
    val moviessRDD = sc.textFile(dataPath + "movies.dat")
    val occupationsRDD = sc.textFile(dataPath + "occupation.dat")
    val ratingsRDD = sc.textFile(dataPath + "ratings.dat")

    //统计不同性别，不同年龄的电影观看人数
    val schemaforusers = StructType("userID::Gender::Age::Occupation::Zip-code".split("::")
      .map(column => StructField(column, StringType, true)))
    val userRDDRows = usersRDD.map(_.split("::")).map(line => Row(line(0).trim, line(1).trim,
      line(2).trim, line(3).trim, line(4).trim))

    val usersDataFrame = spark.createDataFrame(userRDDRows, schemaforusers)

    //ratings.dat  UserID::MovieID::Rating::Timestamp
    val schemaforrating = StructType("UserID::MovieID::Rating::Timestamp".split("::")
      .map(column => StructField(column, StringType, true)))
    val ratinsRDDRows = ratingsRDD.map(_.split("::")).map(line => Row(line(0).trim, line(1).trim,
      line(2).trim, line(3).trim))

    val ratinsDataFrame = spark.createDataFrame(ratinsRDDRows, schemaforrating)
    ratinsDataFrame.filter(s" MovieID = 1193 ")
      .join(usersDataFrame, "UserID")
      .select("Gender", "Age")
      .groupBy("Gender", "Age")
      .count()
      .show(10)

//    while (true) {}
    sc.stop()

  }
}
