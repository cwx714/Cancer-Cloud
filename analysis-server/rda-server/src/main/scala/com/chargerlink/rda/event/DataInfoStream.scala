package com.chargerlink.rda.event

import com.chargerlink.gateway.bean.data.{DataInfo, DataRecorder, DataVehicle}

import kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.{HashMap, HashSet}

/**
  * Created by Cuiwx on 2017/6/13/013.
  */
object DataInfoStream{
  def main(args: Array[String]) {
    val broker = args(0) //"hdp71.chargerlink.com:6667"
    val topicName = args(1) //"cf01"

    val sparkConf = new SparkConf().setAppName("DataInfoStream")
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
//    val dataRecorderList = lines.filter(_.size > 100).map(DataRecorder.parseFrom)
//    val dataVehicleList = lines.filter(_.size > 100).map(DataVehicle.parseFrom)

    /**
    * 终端设备实时状态数据的消息定义。其中：上报消息详情的定义分别在以下文件中：DataVehicle.proto、DataCharger.proto、DataRecorder.proto。
    * deviceType 必填)终端设备类型。
    * deviceId 必填)终端设备ID。格式由对应类型设备自行定义, 设备类型和设备ID组成设备的全局唯一标识。
    * subDevice 必填)子设备标识信息。其中：主设备标识为""，充电枪的标识为"plug=充电枪ID"，地锁的标识为"lock=地锁ID"，车位传感器的标识为"detcet=传感器ID"。
    * dataTime (必填)数据采集的时间(北京时间的毫秒时间戳)。单位：毫秒。
    * dataType (必填)数据类型。数据值为DataType常量的按位组合。
    */
    val dataInfoRDD = dataInfoList.map(dataInfo => {
      (dataInfo.deviceId,dataInfo.subDevice,dataInfo.dataTime,dataInfo.dataType,
        dataInfo.getDataVehicle.getOverallData.powerStatus,
        dataInfo.getDataVehicle.getOverallData.currentSpeed,
        dataInfo.getDataVehicle.getOverallData.currentMileage
      )
    })
    dataInfoRDD.window(Seconds(5)).print()
    dataInfoRDD.countByValueAndWindow(Seconds(20), Seconds(5)).print()

//    val dataRecorderRDD = dataRecorderList.map(dataRecorder => {
//      (dataRecorder.trackId)
//    })
//    dataRecorderRDD.countByValueAndWindow(Seconds(20), Seconds(5)).print()

//    val dataVehicleRDD = dataVehicleList.map(dataVehicle => {
//      (dataVehicle.getOverallData.currentMileage)
//    })
//    dataVehicleRDD.countByValueAndWindow(Seconds(20), Seconds(5)).print()

    ssc.checkpoint("/user/spark/check-point")
    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }
}
