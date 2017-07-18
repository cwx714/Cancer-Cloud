package com.chargerlink.rda.demo

import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * `$ bin/run-example org.apache.spark.examples.streaming.clickstream.PageViewGenerator 44444 10`
  * `$ bin/run-example org.apache.spark.examples.streaming.clickstream.PageViewStream errorRatePerZipCode localhost 44444`
  */
object PageViewStream {
  def setStreamingLogLevels() {
    val log4jInitialized = Logger.getRootLogger.getAllAppenders.hasMoreElements
    if (!log4jInitialized) {
      Logger.getRootLogger.setLevel(Level.WARN)
    }
  }

  def main(args: Array[String]) {

    setStreamingLogLevels()

    val metric = args(0) //pageCounts slidingPageCounts errorRatePerZipCode activeUserCount popularUsersSeen
    val broker = args(1)
    val topic = args(2)
//    val broker = "hdp21.cancer.com:6667"
//    val topic = "rda01"

    // Create the context
    val sparkConf = new SparkConf().setAppName("PageViewStream")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val kafkaParams = Map[String, String]("metadata.broker.list" -> broker)
    val topics = Set(topic)
    val pageViews = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topics)
      .map(_._2).flatMap(_.split("\n")).map(PageView.fromString(_))

    // Return a count of views per URL seen in each batch
    val pageCounts = pageViews.map(view => view.url).countByValue()

    // Return a sliding window of page views per URL in the last ten seconds
    val slidingPageCounts = pageViews.map(view => view.url)
      .countByValueAndWindow(Seconds(20), Seconds(5))


    // Return the rate of error pages (a non 200 status) in each zip code over the last 30 seconds
    val statusesPerZipCode = pageViews.window(Seconds(30), Seconds(5))
      .map(view => ((view.zipCode, view.status)))
      .groupByKey()

    val errorRatePerZipCode = statusesPerZipCode.map{
      case(zip, statuses) =>
        val normalCount = statuses.filter(_ == 200).size
        val errorCount = statuses.size - normalCount
        val errorRatio = errorCount.toFloat / statuses.size
        if (errorRatio > 0.05) {
          "%s: **%s**".format(zip, errorRatio)
        } else {
          "%s: %s".format(zip, errorRatio)
        }
    }

    // Return the number unique users in last 15 seconds
    val activeUserCount = pageViews.window(Seconds(15), Seconds(5))
      .map(view => (view.userID, 1))
      .groupByKey()
      .count()
      .map("Unique active users: " + _)

    // An external dataset we want to join to this stream
    val userList = ssc.sparkContext.parallelize(
      Map(1 -> "Patrick Wendell", 2 -> "Reynold Xin", 3 -> "Matei Zaharia").toSeq)

    metric match {
      case "pageCounts" => pageCounts.print()
      case "slidingPageCounts" => slidingPageCounts.print()
      case "errorRatePerZipCode" => errorRatePerZipCode.print()
      case "activeUserCount" => activeUserCount.print()
      case "popularUsersSeen" =>
        // Look for users in our existing dataset and print it out if we have a match
        pageViews.map(view => (view.userID, 1))
          .foreachRDD((rdd, time) => rdd.join(userList)
            .map(_._2._2)
            .take(10)
            .foreach(u => println("Saw user %s at time %s".format(u, time))))
      case _ => println("Invalid metric entered: " + metric)
    }

    pageCounts.print()
    slidingPageCounts.print()
    errorRatePerZipCode.print()
    activeUserCount.print()
    pageViews.map(view => (view.userID, 1))
      .foreachRDD((rdd, time) => rdd.join(userList)
        .map(_._2._2)
        .take(10)
        .foreach(u => println("Saw user %s at time %s".format(u, time))))

    ssc.checkpoint("/user/spark/check-point")
    ssc.start()
    ssc.awaitTermination()
  }
}
// scalastyle:on println
