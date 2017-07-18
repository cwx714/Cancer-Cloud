package com.chargerlink.rda.event

import java.text.SimpleDateFormat

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * 车辆行驶记录统计表
  * accessId  开始时间	结束时间 开始里程	结束里程	行驶总里程
  *
  * /usr/hdp/2.6.0.3-8/spark2/bin/spark-submit --class com.chargerlink.rda.event.VehicleRecordStatistics
  * --master local[*] /usr/local/app/rda-server-jar-with-dependencies.jar "file:/usr/local/app/csv/vehicleRecord"
  *
  * Created by Cuiwx on 2017/6/28/028.
  */
object VehicleRecordStatistics {
  def main(args: Array[String]) {
    val SPLIT_STR = "-" //"\t"
    val DATE_PARTTEN = "yyyy-MM-dd HH:mm"

    val spark = SparkSession.builder()
      .appName("VehicleRecordStatistics")
      .master("local[*]") //部署生产环境时注释
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val odPath = "/user/spark/rda/vehicle/*/*"
    val overallData = spark.sparkContext.textFile(odPath).map(_.split(SPLIT_STR))
      .map(a => OverallData(a(0), a(1), new SimpleDateFormat(DATE_PARTTEN).format(a(2).toLong), a(3).toInt, a(4).toInt))

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
//recordNo,
    val selAllSql = "select accessId,dataTime,currentSpeed,currentMileage from vehicleRecord " +
//    "where start_time EQ '2017-06-23 14:21'"
//    "where accessId = '4096:LNJS6543223453410' " +
    "order by accessId,dataTime "
    val allResults = spark.sql(selAllSql).distinct()
    allResults.show(false)
    allResults.repartition(1).write.mode(SaveMode.Append).format("csv").save(args(1))
//    allResults.coalesce(1).write.mode(SaveMode.Append).format("csv").save("/user/spark/rda/all-vr")

//    val cntSql = "select count(*) from vehicleRecord"
//    val cntResults = spark.sql(cntSql)
//    cntResults.show()//4770

    spark.stop()
  }
}

case class OverallData(accessId:String, recordNo:String, dataTime:String, currentSpeed:Int, currentMileage:Int)
