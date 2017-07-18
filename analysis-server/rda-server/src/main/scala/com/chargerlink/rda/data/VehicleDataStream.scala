package com.chargerlink.rda.data

import kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, State, StateSpec, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils

import scala.collection.mutable.{HashMap, HashSet, ListBuffer}

/**
  * /usr/hdp/2.6.0.3-8/spark2/bin/spark-submit --class com.chargerlink.rda.data.VehicleDataStream
  * --master local[*] /usr/local/app/rda-server-jar-with-dependencies.jar "hdp71.chargerlink.com:6667" subscribe "cf01"
  *
  * Created by Cuiwx on 2017/7/11/07.
  */
object VehicleDataStream {

  def main(args: Array[String]): Unit = {
    val SPLIT_STR = "-"

    val broker = args(0)
    val topicName = args(1)

    val sparkConf = new SparkConf().setAppName("VehicleDataStream")
    val sparkContext = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sparkContext, Seconds(5))
    ssc.sparkContext.setLogLevel("ERROR")

    val kafkaParameters= new HashMap[String,String]()
    kafkaParameters.put("metadata.broker.list",broker)
    kafkaParameters.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "smallest")// earliest  largest
    kafkaParameters.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "kafka.serializer.StringEncoder");

    val topic = new HashSet[String]()
    topic.add(topicName)
    val stream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParameters.toMap, topic.toSet)
    val lines = stream.map(_._2)

    val dataInfoETL = lines.transform(rdd => {
      rdd.map(_.split(SPLIT_STR)).map(vr => (vr(0), vr(1), vr(2), vr(3), vr(4)))
    })

    val dataInfoMap = dataInfoETL.map(x => (x._1,(x._2.toInt, x._3.toLong, x._4.toInt, x._5.toInt)))
      .groupByKey()
      .map(t => {
        t._2.map(f => {
          var vrNo = 1
          var diffMileage = f._4.toInt//(f._4.toInt.max - f._4.toInt.min)
          if(f._1.toInt == 1) vrNo += 1
          (vrNo, f._1.toInt, f._2.toLong, f._3.toInt, diffMileage )//(f._2.max - f._2.min)
        }).filter(p => (p._2 == 1))
      })

    dataInfoMap.print()

    ssc.checkpoint("/user/spark/check-point")
    ssc.start()
    ssc.awaitTermination()
    ssc.stop()

/**
  *   * Consumes messages from one or more topics in Kafka and does wordcount.
  * Usage: StructuredKafkaWordCount <bootstrap-servers> <subscribe-type> <topics>
  *   <bootstrap-servers> The Kafka "bootstrap.servers" configuration. A
  *   comma-separated list of host:port.
  *   <subscribe-type> There are three kinds of type, i.e. 'assign', 'subscribe',
  *   'subscribePattern'.
  *   |- <assign> Specific TopicPartitions to consume. Json string
  *   |  {"topicA":[0,1],"topicB":[2,4]}.
  *   |- <subscribe> The topic list to subscribe. A comma-separated list of
  *   |  topics.
  *   |- <subscribePattern> The pattern used to subscribe to topic(s).
  *   |  Java regex string.
  *   |- Only one of "assign, "subscribe" or "subscribePattern" options can be
  *   |  specified for Kafka source.
  *   <topics> Different value format depends on the value of 'subscribe-type'.
  */
    //    if (args.length < 3) {
//      System.err.println("Usage: VehicleDataStream <bootstrap-servers> " +
//        "<subscribe-type> <topics>")
//      System.exit(1)
//    }
//
//    val Array(bootstrapServers, subscribeType, topics) = args
//
//    val spark = SparkSession
//      .builder
//      .appName("VehicleDataStream")
//      .getOrCreate()
//
//    import spark.implicits._
//    var vehicle:VehicleEntity  = null
//
//    val ds1 = spark
//      .read
//      .format("kafka")
//      .option("kafka.bootstrap.servers", "hdp71.chargerlink.com:6667")
//      .option("subscribePattern", "cf01")
//      .option("startingOffsets", "earliest")
//      .option("endingOffsets", "latest")
//      .load()
//    ds1.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
//      .as[(String, String)]
//    ds1.show(false)

  }
}
