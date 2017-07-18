package com.chargerlink.rda.demo

import java.util.Properties

import kafka.producer.{KeyedMessage, Producer, ProducerConfig}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Duration, StreamingContext}

/**
  * Created by Cuiwx on 2017/6/9/009.
  */
object MockKafkaProducer {
  def main(args: Array[String]){
    val broker = util.Try(args(0)).getOrElse("hdp71.chargerlink.com:6667")
    val topic = util.Try(args(1)).getOrElse("cf01")

    val sparkConf = new SparkConf().setAppName("MockKafkaProducer")
    val scc = new StreamingContext(sparkConf, Duration(5000))
//    scc.sparkContext.setLogLevel("ERROR")

    val props:Properties = new Properties()
    props.put("metadata.broker.list", broker)
//    props.put("serializer.class", "kafka.serializer.StringEncoder")
    props.put("serializer.class", "kafka.serializer.DefaultEncoder");
    props.put("key.serializer.class", "kafka.serializer.StringEncoder");
    props.put("producer.type", "sync");
    props.put("request.required.acks", "1");

    val config = new ProducerConfig(props)
//    val producer = new Producer[String, String](config)
//    for (i <- 1 to 1000) {
//      producer.send(new KeyedMessage[String, String](topic, "1465210351273 192.168.112.250 4856 98 Jiangsu Nanjing"))
//    }

    val producer = new Producer[String, Array[Byte]](config)
    for (i <- 1 to 100) {
      var pid = "70" + i.toString
      var pname = "person" + i.toString
      var sex = i%5
      var age = i%2
//      var person = Person(pid, pname, sex, age)
//      val msg = person.toByteArray
      var msg = pid + " " + pname + " " + sex + " " + age
      producer.send(new KeyedMessage[String, Array[Byte]](topic, msg.getBytes()))
    }
  }
}

//case class Person(pid: String, pname: String, sex: Int, age: Int)
