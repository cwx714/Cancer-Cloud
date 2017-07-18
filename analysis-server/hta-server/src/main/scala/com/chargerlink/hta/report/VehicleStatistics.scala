package com.chargerlink.hta.report

import java.io.FileInputStream
import java.text.SimpleDateFormat

import org.apache.spark.sql.functions.{countDistinct, sum}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 车辆行驶统计表
  * 汽车品牌 运营企业名称 次均行驶里程 日均行驶里程 次均行驶时长 日均行驶时长 日均出行次数
  *
  * /usr/hdp/2.6.0.3-8/spark2/bin/spark-submit
  * --class com.chargerlink.hta.report.VehicleStatistics
  * --master local[*] --files /usr/local/app/hta-config.properties
  * /usr/local/app/hta-server-jar-with-dependencies.jar
  * "file:/usr/local/app/vehicle-statistics.json" "file:/usr/local/app/csv/vehicle"
  *
  * Created by Cuiwx on 2017/6/16/016.
  */
object VehicleStatistics {

  def main(args: Array[String]){
    val SPLIT_STR = "-" //"\t"
    val DATE_PARTTEN = "yyyy-MM-dd HH:mm"

    val spark = SparkSession.builder()
      .appName("VehicleStatistics")
      .master("local[*]")//部署生产环境时注释
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    val vehicleJson = args(0)
    val vehicleDF = spark.read.json(vehicleJson)
//    val vehicleSel = vehicleDF.select("accessId", "operator", "brand", "vin", "plate")
//    vehicleSel.createGlobalTempView("vehicleCord")

    val odPath = args(1)
//    val odPath = "/user/spark/rda/vehicle/*/*"

    val overallData = spark.sparkContext.textFile(odPath).map(_.split(SPLIT_STR))
      .map(a => VehicleOverallData(a(0), a(1), a(2).toLong, a(3).toInt, a(4).toInt))
    val overallDF = spark.sqlContext.createDataFrame(overallData).toDF()
    overallDF.createOrReplaceTempView("dataVehOverall")

    /**
      * User Defined Untyped Aggregation
      */
    spark.udf.register("vehicleAverage", VehicleAverage)

    /**
      * User Defined Typed Aggregation
      */
    //    val dataVehOverallDF = spark.read.json(dataVehOverallJson).as[DataVehOverall]
    //    dataVehOverallDF.show()
    //    val vehicleMileageAverage = VehicleAverageAsUserDefined.toColumn.name("currentMileage")
    //    val result = dataVehOverallDF.select(vehicleMileageAverage)
    //    result.show()

    val vehicleNumSql = "select dv.accessId,vr.brand,vr.operator," +
      "vehicleAverage(dv.currentMileage) average_mileage_num," +
      "vehicleAverage(dv.dataTime) mean_travel_time_num," +
      "count(dv.accessId) average_daily_trip_times_num " +
      "from dataVehOverall dv,global_temp.vehicleCord vr " +
      "where dv.accessId = vr.accessId " +
      "group by dv.accessId,dv.recordNo,vr.brand,vr.operator "

    val numResults = spark.sql(vehicleNumSql)

    val vehicleDaySql = "select dv.accessId,vr.brand,vr.operator," +
      "vehicleAverage(dv.currentMileage) average_mileage_day," +
      "vehicleAverage(dv.dataTime) mean_travel_time_day," +
      "count(dv.accessId) average_daily_trip_times_day " +
      "from dataVehOverall dv,global_temp.vehicleCord vr " +
      "where dv.accessId = vr.accessId " +
      "group by dv.accessId,vr.brand,vr.operator"

    val dayResults = spark.sql(vehicleDaySql)

//    val vehicleSql = "(select dv.accessId,vr.brand,vr.operator," +
//      "vehicleAverage(dv.currentMileage) average_mileage_day," +
//      "vehicleAverage(dv.dataTime) mean_travel_time_day," +
//      "count(dv.accessId) average_daily_trip_times_day " +
//      "from dataVehOverall dv,global_temp.vehicleCord vr " +
//      "where dv.accessId = vr.accessId " +
//      "group by dv.accessId,vr.brand,vr.operator) vd " +
//      "left join " +
//      "(select dv.accessId,dv.recordNum,vr.brand,vr.operator," +
//      "vehicleAverage(dv.currentMileage) average_mileage_num," +
//      "vehicleAverage(dv.dataTime) mean_travel_time_num," +
//      "count(dv.accessId) average_daily_trip_times_num " +
//      "from dataVehOverall dv,global_temp.vehicleCord vr " +
//      "where dv.accessId = vr.accessId " +
//      "group by dv.accessId,dv.recordNum,vr.brand,vr.operator) vn " +
//      "on vd.accessId = vn.accessId"
//
//    val results = spark.sql(vehicleSql)

    /**
      * 查询结果存入CSV文件
      */

    val vehicleNumCsvPath = args(2)
    numResults.coalesce(1).write.mode(SaveMode.Append).format("csv").save(vehicleNumCsvPath)
    val vehicleDayCsvPath = args(3)
    dayResults.coalesce(1).write.mode(SaveMode.Append).format("csv").save(vehicleDayCsvPath)

    numResults.show()
    dayResults.show()

//    val results = numResults.join(dayResults)
//    val vehicleCsvPath = args(1)
//    results.coalesce(1).write.mode(SaveMode.Append).format("csv").save(vehicleCsvPath)
//    println("汽车品牌\t运营企业名称\t次均行驶里程\t日均行驶里程\t次均行驶时长\t日均行驶时长\t日均出行次数")
//    results.show()

    /**
      * 查询结果存入数据库
      */
//    val filePath = "/usr/local/app/hta-config.properties"
//    val devProperties = new java.util.Properties
//    devProperties.load(new FileInputStream(filePath))
//    val url = devProperties.getProperty("url")
//    val user = devProperties.getProperty("user")
//    val password = devProperties.getProperty("password")
//    val driver = devProperties.getProperty("driver")
//    val vehicleTable = devProperties.getProperty("hta.v_table")
//
//    vsResults.write.mode(SaveMode.Append).jdbc(url, vehicleTable, devProperties)

    spark.stop()
  }

}

case class VehicleOverallData(accessId:String, recordNo:String, dataTime:Long, currentSpeed:Int, currentMileage:Int)