package com.chargerlink.rda.demo

import com.chargerlink.gateway.bean.data.DataInfo
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.ProcessingTime

/**
  * Created by Cuiwx on 2017/6/27/027.
  */
object StructuredStreamingKafka {
  def main(args: Array[String]) {
    val broker = args(0) //"hdp71.chargerlink.com:6667"
    val topicName = args(1) //"cf01"

    val spark = SparkSession.builder().appName("StructuredStreamingKafka")
      .master("local[*]")//部署生产环境时注释
      .getOrCreate()

    import spark.implicits._

    // Create DataSet representing the stream of input lines from kafka
    val lines = spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", broker)
      .option("startingoffset", "smallest")
      .option("topics", topicName)
//      .option("value.serializer","org.common.serialization.StringSerializer")
//      .option("key.serializer","org.common.serialization.StringSerializer")
      .load()
      //.selectExpr("CAST(value AS StringType)")
      //.as[String]

    val dataInfoList = lines.filter(_.size > 100)
    val dataInfoRDD = dataInfoList.transform(rdd => {
      rdd.map(dataInfo => {
        val rs = dataInfo.get(0) + "-" + dataInfo.get(1) + "-" + dataInfo.get(2) + "-" + dataInfo.get(3)
        rs
      })
    })
    println(dataInfoRDD)

//    val dataInfoRDD = dataInfoList.transform(rdd => {
//      rdd.map(dataInfo => {
//        (dataInfo.deviceId, dataInfo.subDevice, dataInfo.dataTime, dataInfo.dataType,
//          dataInfo.getDataVehicle.getOverallData.powerStatus,
//          dataInfo.getDataVehicle.getOverallData.currentSpeed,
//          dataInfo.getDataVehicle.getOverallData.currentMileage)
//      })
//    })
//    dataInfoRDD.print()


  }
}
