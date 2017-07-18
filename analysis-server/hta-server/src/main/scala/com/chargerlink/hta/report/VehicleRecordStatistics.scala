package com.chargerlink.hta.report

import java.io.FileInputStream
import java.text.SimpleDateFormat

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * 车辆行驶记录统计表
  * 汽车品牌	运营企业名称	车牌号	vin码	开始里程	结束里程	行驶总里程	开始时间	结束时间
  *
  * /usr/hdp/2.6.0.3-8/spark2/bin/spark-submit 
  * --class com.chargerlink.hta.report.VehicleRecordStatistics
  * --master local[*] --files /usr/local/app/hta-config.properties
  * /usr/local/app/hta-server-jar-with-dependencies.jar
  * "file:/usr/local/app/vehicle-statistics.json" "file:/usr/local/app/dataVehOverall.json" "file:/usr/local/app/csv/vehicleRecord"
  *
  * Created by Cuiwx on 2017/6/16/016.
  */
object VehicleRecordStatistics {

  def main(args: Array[String]){
    val SPLIT_STR = "-" //"\t"
    val DATE_PARTTEN = "yyyy-MM-dd HH:mm"

    val spark = SparkSession.builder()
      .appName("VehicleRecordStatistics")
      .master("local[*]")//部署生产环境时注释
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val odPath = args(0)
//    val odPath = "/user/spark/rda/vehicle/*/*"

    val overallData = spark.sparkContext.textFile(odPath).map(_.split(SPLIT_STR))
      .map(a => VehicleRecord(a(0), a(1), new SimpleDateFormat(DATE_PARTTEN).format(a(2).toLong), a(3).toInt, a(4).toInt))

    val overallDF = spark.sqlContext.createDataFrame(overallData).toDF()
    overallDF.createOrReplaceTempView("vehicleRecord")

    //	accessId  开始时间	结束时间 开始里程	结束里程	行驶总里程
    val dataVehOverallSql = "select accessId," +
      "min(dataTime) start_time,max(dataTime) stop_time," +
      "min(currentMileage) start_mileage,max(currentMileage) stop_mileage," +
      "sum(currentMileage) total_mileage from vehicleRecord group by accessId,recordNo "
    val vsResults = spark.sql(dataVehOverallSql)
    vsResults.show(false)
    vsResults.repartition(1).write.mode(SaveMode.Append).format("csv").save(args(0))

    spark.stop()
  }
}

case class VehicleRecord(accessId:String, recordNo:String, dataTime:String, currentSpeed:Int, currentMileage:Int)