package com.chargerlink.rda.demo

//import kafka.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerConfig
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka._
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}

import scala.collection.mutable._

/**
  * Created by Cuiwx on 2017/6/8/007.
  */
object SparkStreamingOnKafkaDirect {
 def main (args: Array[String]){
    val broker = args(0) //"hdp71.chargerlink.com:6667"
    val topicName = args(1) //"cf01"
    val conf = new SparkConf().setAppName("SparkStreamingOnKafkaDirect")
//    val ssc = new StreamingContext(conf,Seconds(10))
    val ssc = new StreamingContext(conf, Duration(5000))
    ssc.sparkContext.setLogLevel("ERROR")

    val kafkaParameters= new HashMap[String,String]()
    kafkaParameters.put("metadata.broker.list",broker)
    kafkaParameters.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "smallest")// earliest  largest
    kafkaParameters.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer")// earliest

    val topic = new HashSet[String]()
    topic.add(topicName)

    val lines = KafkaUtils.createDirectStream[String,String,StringDecoder, StringDecoder](ssc,kafkaParameters.toMap,topic.toSet).map(_._2)
    val words = lines.flatMap{_.split(" ")}
    val pairs = words.map(word=>(word,1))
    val wordsCount = pairs.reduceByKey(_+_)

    wordsCount.print()
    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
 }
}
