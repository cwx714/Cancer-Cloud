package com.chargerlink.rda.event

import com.chargerlink.gateway.bean.data.DataInfo
import kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.HashSet
import collection.mutable.HashMap

/**
  * 车辆行驶记录统计实时流
  * [DataInfo]
  * 终端设备实时状态数据的消息定义。其中：上报消息详情的定义分别在以下文件中：DataVehicle.proto、DataCharger.proto、DataRecorder.proto。
  * deviceType 必填)终端设备类型。
  * deviceId 必填)终端设备ID。格式由对应类型设备自行定义, 设备类型和设备ID组成设备的全局唯一标识。
  * subDevice 必填)子设备标识信息。其中：主设备标识为""，充电枪的标识为"plug=充电枪ID"，地锁的标识为"lock=地锁ID"，车位传感器的标识为"detcet=传感器ID"。
  * dataTime (必填)数据采集的时间(北京时间的毫秒时间戳)。单位：毫秒。
  * dataType (必填)数据类型。数据值为DataType常量的按位组合。
  *
  * [OverallData]
  * power_status  车辆状态。0x01：启动状态，0x02：熄火状态，0x03：其他状态，0xFE：异常，0xFF：无效。
  * charge_status 充电状态。0x01：停车充电，0x02：行驶充电，0x03：未充电状态，0x04：充电完成，0xFE：异常，0xFF：无效。
  * fuel_status 动力运行模式。0x01: 纯电，0x02：混动，0x03：燃油，0xFE：异常，0xFF：无效。
  * current_speed 当前里程表车速。单位：0.1km/h，范围：0.0km/h～220.0km/h，0xFFFE：异常，0xFFFF：无效。
  * current_mileage 当前里程表读数。单位：0.1km，范围：0.0km～999999.9km，0xFFFFFFFE：异常，0xFFFFFFFF：无效。
  * total_voltage 总电压。单位：0.1V，范围：0.0V～1000.0V，0xFFFE：异常，0xFFFF：无效。
  * total_current 总电流。单位：0.1A，偏移：-1000.0A，范围：-1000.0A～1000.0A，0xFFFE：异常，0xFFFF：无效。
  * soc_status SOC状态。单位：1%，范围：0%～100%，0xFE：异常，0xFF：无效。
  * dc_status DC-DC状态。0x01：工作，0x02：断开，0xFE：异常，0xFF：无效。
  * gear_status 档位状态。Bit5：1-驱动有效/0-驱动无效，Bit4：1-制动有效/0-制动无效，Bit0～3：0-空挡/1～12-1档～12档/13-倒车挡/14-自动D档/15-停车P档，0xFE：异常，0xFF：无效。
  * resistance 绝缘电阻值。单位：1kΩ，范围：0kΩ～60000kΩ，0xFFFE：异常，0xFFFF：无效。
  * power_value 牵引踏板信号。单位：1%，范围：0%～100%，0xFE：异常，0xFF：无效。
  * brake_value 制动踏板信号。单位：1%，范围：0%～100%，0xFE：异常，0xFF：无效。
  *
  * Created by Cuiwx on 2017/6/23/023.
  */
object VehicleRecordStream {
  def main(args: Array[String]) {
    val SPLIT_STR = "-" //"\t"

    val broker = args(0) //"hdp71.chargerlink.com:6667"
    val topicName = args(1) //"cf01"

    val sparkConf = new SparkConf().setAppName("VehicleRecordStream")
    val sparkContext = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sparkContext, Seconds(5))
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

    //分组统计次数
    var recordNum = 0
    var recordNo = ""
    var diffMileage = 0

//    val dataInfoDvod = dataInfoList.transform(rdd => {
//      rdd.map ({ dataInfo =>
//        val accessId = dataInfo.dataType + ":" + dataInfo.deviceId
//        val dataTime = dataInfo.dataTime
//        val currentSpeed = dataInfo.getDataVehicle.getOverallData.currentSpeed
//        val currentMileage = dataInfo.getDataVehicle.getOverallData.currentMileage
//
//        (accessId, dataTime + SPLIT_STR + currentSpeed + SPLIT_STR + currentMileage)
//      })
//    }).groupByKey()
//    dataInfoDvod.print()
//
//    val dataInfoPs = dataInfoList.transform(rdd => {
//      rdd.map ({ dataInfo =>
//        val accessId = dataInfo.dataType + ":" + dataInfo.deviceId
//        var powerStatus = dataInfo.getDataVehicle.getOverallData.powerStatus
//
//        (accessId,powerStatus)
//      })
//    }).groupByKey()
////    dataInfoPs.print()
//
//    val dataInfoCm = dataInfoList.transform(rdd => {
//      rdd.map ({ dataInfo =>
//        val accessId = dataInfo.dataType + ":" + dataInfo.deviceId
//        val currentMileage = dataInfo.getDataVehicle.getOverallData.currentMileage
//
//        (accessId,currentMileage)
//      })
//    }).groupByKey()
//    dataInfoCm.print()

//    val dataInfoNum = dataInfoPs.map(f => {
//      val psKey = f._1
//      val psValue = f._2
//      psValue.foreach(v => {
//        if (v == 1) recordNum == 0
//        if (v == 2) recordNum += 1
//      })
//      (psKey, recordNum)
//    })
//    dataInfoNum.print() //(4096:LNJS6543223453453,0)(4096:LNJS6543223453454,482)

//    val dataInfoDvodNum = dataInfoDvod.join(dataInfoNum)
//        dataInfoDvodNum.foreachRDD(rdd => {
//          println("rdd.count(): " + rdd.count())
    //      rdd.map(f => {
    //        println("f._1: " + f._1)
    //        println("f._2._1: " + f._2._1)
    //        println("f._2._2: " + f._2._2)
    //      })
//        })
//    dataInfoDvodNum.print()

    val dataInfoETL = dataInfoList.transform(rdd => {
      rdd.map ({ dataInfo =>
        val accessId = dataInfo.dataType + ":" + dataInfo.deviceId
        val dataTime = dataInfo.dataTime
        var powerStatus = dataInfo.getDataVehicle.getOverallData.powerStatus
        val currentSpeed = dataInfo.getDataVehicle.getOverallData.currentSpeed
        val currentMileage = dataInfo.getDataVehicle.getOverallData.currentMileage

//        if(powerStatus == 2) {
//          recordNum += 1
//        }
//        recordNo = dataInfo.deviceId + ":" + recordNum

        if(powerStatus == 1) {
          recordNum = 1
          diffMileage = currentMileage - diffMileage //100-0=100 200-100=100
          println("diffMileage: " + diffMileage)
        }

//        recordNum = powerStatus - recordNum //1-0=1 2-0=2 2-1=1
//        println("recordNum: " + recordNum)

        (accessId + SPLIT_STR + recordNum + SPLIT_STR + dataTime + SPLIT_STR + currentSpeed + SPLIT_STR + diffMileage)
        //(accessId + SPLIT_STR + recordNum + SPLIT_STR + dataTime + SPLIT_STR + currentSpeed + SPLIT_STR + currentMileage)
        //(accessId + SPLIT_STR + recordNo + SPLIT_STR + dataTime + SPLIT_STR + currentSpeed + SPLIT_STR + currentMileage)
        //(accessId + SPLIT_STR + powerStatus + SPLIT_STR + dataTime + SPLIT_STR + currentSpeed + SPLIT_STR + currentMileage)
      })//.distinct()
    })

    dataInfoETL.print(30)
//    dataInfoETL.window(Seconds(10)).saveAsTextFiles("/user/spark/rda/vehicle/")

//    val dataInfoRs = dataInfoList.transform(rdd => {
//      rdd.map(dataInfo =>
//        (dataInfo.dataType + ":" + dataInfo.deviceId + SPLIT_STR + dataInfo.dataTime + SPLIT_STR
//          + dataInfo.getDataVehicle.getOverallData.powerStatus + SPLIT_STR
//          + dataInfo.getDataVehicle.getOverallData.currentSpeed + SPLIT_STR
//          + dataInfo.getDataVehicle.getOverallData.currentMileage
//          )
//      )
//    })
//    dataInfoRs.window(Seconds(10)).saveAsTextFiles("/user/spark/rda/vehicle/")

    ssc.checkpoint("/user/spark/check-point")
    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }
}
