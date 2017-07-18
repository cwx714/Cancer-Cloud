package com.chargerlink.rda.demo

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Duration, StreamingContext}

/**
  * Created by Cuiwx on 2017/6/8/007.
  */
object KafkaSparkDemo {
  def main(args: Array[String]) {
    val broker = args(0) //"hdp71.chargerlink.com:6667"
    val topicName = args(1) //"cf01"
    val sparkConf = new SparkConf().setAppName("kafka-spark-demo")
    val ssc = new StreamingContext(sparkConf, Duration(5000))
    ssc.sparkContext.setLogLevel("ERROR")
    ssc.checkpoint("/data/checkpoint") // 因为使用到了updateStateByKey,所以必须要设置checkpoint
    val topics = Set(topicName) //我们需要消费的kafka数据的topic
    val brokers = broker
    val kafkaParam = Map[String, String](
      //      "zookeeper.connect" -> "192.168.21.181:2181",
      //      "group.id" -> "test-consumer-group",
      "metadata.broker.list" -> brokers,// kafka的broker list地址
      "serializer.class" -> "kafka.serializer.StringEncoder"
    )

    val stream: InputDStream[(String, String)] = createStream(ssc, kafkaParam, topics)

    stream.map(_._2)      // 取出value
      .flatMap(_.split(" ")) // 将字符串使用空格分隔
      .map(r => (r, 1))      // 每个单词映射成一个pair
      .updateStateByKey[Int](updateFunc)  // 用当前batch的数据区更新已有的数据
      .print() // 打印前10个数据
    ssc.start() // 真正启动程序
    ssc.awaitTermination() //阻塞等待
  }
  val updateFunc = (currentValues: Seq[Int], preValue: Option[Int]) => {
    val curr = currentValues.sum
    val pre = preValue.getOrElse(0)
    Some(curr + pre)
  }
  /**
    * 创建一个从kafka获取数据的流.
    * @param scc           spark streaming上下文
    * @param kafkaParam    kafka相关配置
    * @param topics        需要消费的topic集合
    * @return
    */
  def createStream(scc: StreamingContext, kafkaParam: Map[String, String], topics: Set[String]) = {
    KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](scc, kafkaParam, topics)
  }
}
