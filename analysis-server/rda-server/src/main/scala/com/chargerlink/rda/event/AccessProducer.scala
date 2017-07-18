package com.chargerlink.rda.event

import com.chargerlink.gateway.bean.common.DeviceType
import com.chargerlink.gateway.bean.data.DataInfo
import com.chargerlink.gateway.bean.data.DataVehOverall
import com.chargerlink.gateway.bean.data.DataVehicle

import java.util.Properties

import kafka.producer.{KeyedMessage, Producer, ProducerConfig}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Duration, StreamingContext}

/**
  * Created by Cuiwx on 2017/6/22/022.
  */
object AccessProducer {
  def main(args: Array[String]){
//    val broker = util.Try(args(0)).getOrElse("hdp71.chargerlink.com:6667") //"hdp71.chargerlink.com:6667"
//    val topic = util.Try(args(1)).getOrElse("cf01")

//      val sparkConf = new SparkConf().setAppName("AccessProducer")
//      val scc = new StreamingContext(sparkConf, Duration(5000))
//      scc.sparkContext.setLogLevel("ERROR")
//
//      val props:Properties = new Properties()
//      props.put("metadata.broker.list", broker)
//      props.put("serializer.class", "kafka.serializer.DefaultEncoder")
//      props.put("key.serializer.class", "kafka.serializer.StringEncoder")
//      props.put("producer.type", "sync")
//      props.put("request.required.acks", "1")
//
//      val config = new ProducerConfig(props)
//
//      val producer = new Producer[String, Array[Byte]](config)
    for (i <- 1 to 1000) {

      var deviceType = DeviceType.fromValue(1+i)
      var dataDetail = com.chargerlink.gateway.bean.data.DataInfo.DataDetail

      var dataVehicle = com.chargerlink.gateway.bean.data.DataInfo.DataDetail.DataVehicle.apply(null)
//      var dataVehicle = com.chargerlink.gateway.bean.data.DataInfo.DataDetail.DataVehicle
//      var dataVehOverall = dataVehicle.value.getOverallData
      var dataVehOverall = DataVehOverall(
        powerStatus = if(i%5==0) 2 else 1,
        chargeStatus = 1,
        fuelStatus = 1,
        currentSpeed = i*2,
        currentMileage = i*5,
        totalVoltage = 1,
        totalCurrent = 1,
        socStatus = 1,
        dcStatus = 1,
        gearStatus = 1,
        resistance = 1,
        powerValue = 1,
        brakeValue = 1
      )

      var dataInfo = new DataInfo(
        deviceType = deviceType,
        deviceId = "LJUB0W1N1GS000" + i,
        subDevice = "",
        dataTime = 1493730248000L,
        dataType = 100,
        dataDetail = dataVehicle
      )
      //var dataInfo = DataInfo.defaultInstance
      println(dataInfo.deviceId + " : " + dataInfo.dataTime + " : " + dataInfo.deviceType.value)
      val accessId = dataInfo.dataType + ":" + dataInfo.deviceId
      println("accessId : " + accessId)

      println(dataVehOverall.powerStatus + " : " + dataVehOverall.currentSpeed + " : " + dataVehOverall.currentMileage)

//      var powerStatus = dataVehOverall.powerStatus
//      var currentMileage = dataVehOverall.currentMileage
//      var currentSpeed = dataVehOverall.currentSpeed
//
//      var powerStatus = dataInfo.getDataVehicle.getOverallData.powerStatus
//      var currentMileage = dataInfo.getDataVehicle.getOverallData.currentMileage
//      var currentSpeed = dataInfo.getDataVehicle.getOverallData.currentSpeed
//
//      println(powerStatus + " : " + currentMileage + " : " + currentSpeed)
//
//      val msg = dataInfo.toByteArray
//      producer.send(new KeyedMessage[String, Array[Byte]](topic, msg))
    }
  }
}