package com.chargerlink.hta.demo

import org.apache.spark.SparkConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

/**
  * Created by Cuiwx on 2017/7/11/011.
  */
object MUAnalyzer {
  def main(args: Array[String]) {

    var masterUrl = "local[*]"
    var dataPath = "file:/D:/Work/Code/analysis-server/hta-server/data/"
    if (args.length > 0) {
      masterUrl = args(0)
    } else if (args.length > 1) {
      dataPath = args(1)
    }

    val spark = SparkSession.builder().appName("MUAnalyzer").master(masterUrl).getOrCreate()
    val sc = spark.sparkContext
    val sqlContext = spark.sqlContext
    sc.setLogLevel("WARN")

    val usersRDD = sc.textFile(dataPath + "users.dat")
    val moviessRDD = sc.textFile(dataPath + "movies.dat")
    val occupationsRDD = sc.textFile(dataPath + "occupation.dat")
    val ratingsRDD = sc.textFile(dataPath + "ratings.dat")

//    val userSchemaString = "USERID::GENDER::AGE"
//    val userSchema = StructType(userSchemaString.split("::").map(field => StructField(field, StringType, true)))
//    val userDataFrame = sqlContext.createDataFrame(usersData.map(_.split("::")).map(line => Row(line(0),line(1), line(2))), userSchema)
//    val ratingSchema = StructType("USERID::MOVIEID".split("::").map(field => StructField(field, StringType, true)))
//    val ratingDataFrame = sqlContext.createDataFrame(ratingsData.map(_.split("::")).map(line => Row(line(0),line(1))), ratingSchema)
//    ratingDataFrame.filter(s"MOVIEID = '1'").join(userDataFrame, "USERID").select("GENDER", "AGE").groupBy("GENDER","AGE").count().show

    /**
      * 转换为电影ID，元组（评分数，计数为1）
      * 按电影ID聚合计算，value是元组（评分数，计数为1），value的计算方式：评分相加累计，计数累加
      * 即某部电影， 用户给它评分总分是多少，共评了多少次。
      * 再次转换，分数的总分值 除以 总次数，就是某部电影的平均评分数，形成key value对，key是评分，value是某部电影ID
      * 按评分的分数排序 // take（5）请前五名，显示
      */
    val ratingsMovies = ratingsRDD.map(_.split("::")).map(line => (line(0), line(1), line(2))).cache()
    ratingsMovies.map(x => (x._2,(x._3, 1))).reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2))
    .map(t => (t._2._1.toFloat / t._2._2.toFloat,t._1))
    .sortByKey(false)
    .take(10)
    .foreach(println)

    println("==============================")

    ratingsMovies.map(x => (x._1, 1)).reduceByKey(_+_).map(x => (x._2, x._1)).sortByKey(false)
      .take(10).foreach(println)

    println("==============================")

    val usersBasic = usersRDD.map(_.split("::")).map { user =>
      (
        user(3),
        (user(0), user(1), user(2))
      )
    }

    val occupations = occupationsRDD.map(_.split("::")).map(job => (job(0), job(1)))
    val userInformation = usersBasic.join(occupations)
    userInformation.cache()
    for (elem <- userInformation.collect()) {
      println(elem)
    }

    println("==============================")

    val targetMoive = ratingsRDD.map(_.split("::")).map(x => (x(0), x(1))).filter(_._2.equals("1193"))
    val targetUsers = userInformation.map(x => (x._2._1._1, x._2))
    val userInformationForSpecificMovie = targetMoive.join(targetUsers)
    for (elem <- userInformationForSpecificMovie.collect()) {
      println(elem)
    }

    println("==============================")

    //users.dat   UserID::Gender::Age::Occupation::Zip-code
    //ratings.dat  UserID::MovieID::Rating::Timestamp
    //Occupation    6:  "doctor/health care"
    // movies.dat  MovieID::Title::Genres
    val ratings = ratingsRDD.map(_.split("::")).map(x => (x(0), x(1), x(2))).cache()
    ratings.map(x => (x._2, (x._3.toInt, 1)))
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)) // 总分，总人数
      .map(x => (x._2._1.toDouble / x._2._2, x._1))
      .sortByKey(false)
      .take(10)
      .foreach(println)

    println("==============================")

    //观看人数最多的电影  //ratings.dat  UserID::MovieID::Rating::Timestamp
    ratings.map(x => (x._1, 1)).reduceByKey(_ + _).map(x => (x._2, x._1)).sortByKey(false)
      .take(10).foreach(print)

    //1，"users.dat"：UserID::Gender::Age::OccupationID::Zip-code
    //2，"movies.dat"：MovieID::Title::Genres
    val male = "M"
    val female = "F"
    //ratings.dat  UserID::MovieID::Rating::Timestamp
    val ratingsMap = ratings.map(x => (x._1, (x._1, x._2, x._3)))
    val usersMap = usersRDD.map(_.split("::")).map(x => (x(0),x(1)))
    val genderRatings = ratingsMap.join(usersMap).cache()
//    val genderRatings = ratings.map(x => (x._1, (x._1, x._2, x._3))).join(
//      usersRDD.map(_.split("::")).map(x => (x(0), x(1)))).cache()
    genderRatings.take(10).foreach(println)
    val maleRatings = genderRatings.filter(x => x._2._2.equals("M")).map(x => x._2._1)
    val femaleRatings = genderRatings.filter(x => x._2._2.equals("F")) map (x => x._2._1)

    maleRatings.map(x => (x._2, (x._3.toInt, 1)))
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)) // 总分，总人数
      .map(x => (x._2._1.toDouble / x._2._2, x._1))
      .sortByKey(false)
      .map(x => (x._2, x._1))
      .take(10)
      .foreach(println)

    println("==============================")

    femaleRatings.map(x => (x._2, (x._3.toInt, 1)))
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)) // 总分，总人数
      .map(x => (x._2._1.toDouble / x._2._2, x._1))
      .sortByKey(false)
      .map(x => (x._2, x._1))
      .take(10)
      .foreach(println)

    println("==============================")
  }
}
