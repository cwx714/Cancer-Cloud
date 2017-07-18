package com.chargerlink.rda.demo

import com.chargerlink.gateway.bean.data.{DataInfo,DataRecorder,DataVehicle}

import kafka.serializer.StringDecoder
import kafka.serializer.DefaultDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils

import scala.collection.mutable.{HashMap, HashSet}

/**
  * Created by Cuiwx on 2017/6/13/013.
  */
object MockKafkaConsumer {
  def main(args: Array[String]) {
    val broker = args(0) //"hdp71.chargerlink.com:6667"
    val topicName = args(1) //"cf01"

    val sparkConf = new SparkConf().setAppName("MockKafkaConsumer")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    ssc.sparkContext.setLogLevel("ERROR")

    val kafkaParameters= new HashMap[String,String]()
    kafkaParameters.put("metadata.broker.list",broker)
    kafkaParameters.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "smallest")// earliest  largest
    kafkaParameters.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.ByteArraySerializer")

    val topic = new HashSet[String]()
    topic.add(topicName)
    val stream = KafkaUtils.createDirectStream[String, Array[Byte], StringDecoder, DefaultDecoder](ssc, kafkaParameters.toMap, topic.toSet)
    val lines = stream.map(_._2)

    val dataInfoList = lines.filter(_.size > 100).map(DataInfo.parseFrom)

    val cnt = dataInfoList.map(dataInfo => dataInfo.deviceId).countByValue()
    cnt.print()

    val slidingCounts = dataInfoList.map(dataInfo => dataInfo.deviceId)
      .countByValueAndWindow(Seconds(20), Seconds(5))
    slidingCounts.print()

    val deviceIdAndTime = dataInfoList.window(Seconds(30), Seconds(5))
      .map(dataInfo => (dataInfo.deviceId, dataInfo.dataTime))
      .groupByKey()
    deviceIdAndTime.print()

    val rateDeviceIdAndTime = deviceIdAndTime.map{
      case(deviceId, dataTime) =>
        val normalCount = dataTime.filter(_ == 200).size
        val errorCount = dataTime.size - normalCount
        val errorRatio = errorCount.toFloat / dataTime.size
        if (errorRatio > 0.05) {
          "%s: **%s**".format(deviceId, errorRatio)
        } else {
          "%s: %s".format(deviceId, errorRatio)
        }
    }
    rateDeviceIdAndTime.print()

    ssc.checkpoint("/user/spark/check-point")
    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }
}
