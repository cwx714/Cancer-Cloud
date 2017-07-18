package com.chargerlink.rda.event

import com.chargerlink.gateway.bean.data.DataInfo
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.streaming.Seconds

/**
  * Created by Cuiwx on 2017/6/23/023.
  */
object AccessConsumer {
  def main(args : Array[String]): Unit = {
    val broker = args(0) //"hdp71.chargerlink.com:6667"
    val topicName = args(1) //"cf01"

    val spark = SparkSession.builder.appName("AccessConsumer").getOrCreate()

    import spark.implicits._

    def parseLine(s: String): DataInfo = {
      DataInfo.parseFrom(org.apache.commons.codec.binary.Base64.decodeBase64(s))
    }
    val ds1 = spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers",broker)
      .option("subscribe",topicName).load()
    val ds2 = ds1.selectExpr("CAST(value AS STRING)").as[String]
    val ds3 = ds2.map(str => parseLine(str))
//    val ds3 = ds2.filter(_.size > 100).map(str => parseLine(str))

    val dataInfoRDD = ds3.map(dataInfo => {
      (dataInfo.deviceId,dataInfo.subDevice,dataInfo.dataTime,dataInfo.dataType)
    })
    dataInfoRDD.show()

//    spark.sqlContext.createDataFrame(dataInfoRDD, DataInfo)
//    spark.sqlContext.protoToDataFrame(ds3).registerTempTable("dataInfo")
//    val ds4 = spark.sqlContext.sql("select deviceId from dataInfo")
//    val query = ds4.writeStream
//      .outputMode("append")
//      .format("console")
//      .start()
//    query.awaitTermination()
  }
}
