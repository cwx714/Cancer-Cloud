package com.chargerlink.rda.demo

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by Cuiwx on 2017/6/7/007.
  */
object FileStreamCntApp {
  def main(args: Array[String]) {
    // val filePath = "file:///D:/Work/Project/testData"
    val filePath = args(1)

    val sparkConf = new SparkConf().setAppName("FileStreamCntApp")
    val ssc = new StreamingContext(sparkConf, Seconds(30))

    val lines = ssc.textFileStream(filePath)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
