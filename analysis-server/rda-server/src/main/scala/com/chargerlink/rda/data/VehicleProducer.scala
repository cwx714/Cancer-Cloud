package com.chargerlink.rda.data

import java.util.Properties

import kafka.producer.{KeyedMessage, Producer, ProducerConfig}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Duration, StreamingContext}

/**
  * Created by Cuiwx on 2017/7/11/011.
  */
object VehicleProducer {
  def main(args: Array[String]){
    val broker = util.Try(args(0)).getOrElse("hdp71.chargerlink.com:6667")
    val topic = util.Try(args(1)).getOrElse("cf01")

    val props:Properties = new Properties()
    props.put("metadata.broker.list", broker)
    props.put("serializer.class", "kafka.serializer.StringEncoder")
    props.put("key.serializer.class", "kafka.serializer.StringEncoder");
    props.put("producer.type", "sync");
    props.put("request.required.acks", "1");

    val config = new ProducerConfig(props)
    val producer = new Producer[String, String](config)
    for (i <- 1 to 1000) {
      var accessId = "4096:LNJS6543223453"
      if(i%2 == 0) accessId = accessId + "410"
      else if(i%5 == 0) accessId = accessId + "411"
      else if(i%7 == 0) accessId = accessId + "453"
      else accessId = accessId + "455"

      var powerStatus = 1
      if(i%50 == 0) powerStatus = 2
      var dataTime = 1498197070000L + i*1000/5

      var currentSpeed = i*2000/12
      if(i%2 == 0) currentSpeed = i*2000/2
      else if(i%5 == 0) currentSpeed = i*2000/5
      else if(i%7 == 0) currentSpeed = i*2000/7
      else currentSpeed = i*2000/4

      var currentMileage = i*5000/8

      var msg = accessId + "-" + powerStatus + "-" + dataTime + "-" + currentSpeed + "-" + currentMileage
//      println(msg)
      producer.send(new KeyedMessage[String, String](topic, msg))
    }

  }
}
