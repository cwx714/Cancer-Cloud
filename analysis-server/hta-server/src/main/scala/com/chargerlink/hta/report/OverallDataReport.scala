package com.chargerlink.hta.report

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.functions._
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import scala.collection.mutable

/**
  * Created by Cuiwx on 2017/7/7/007.
  */
object OverallDataReport {

  case class OverallRptData(accessId:String, powerStatus:Int, dataTime:Long, currentSpeed:Int, currentMileage:Int)

  def main(args: Array[String]) {
    val SPLIT_STR = "-" //"\t"
    val DATE_PARTTEN = "yyyy-MM-dd HH:mm"

    val spark = SparkSession.builder()
      .appName("OverallDataReport")
      .master("local[*]") //部署生产环境时注释
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    val vrPath = args(0)
//    val vrPath = "/user/spark/rda/vr76/*"
//    val vrPath = "file:/D:/Work/Code/analysis-server/hta-server/data/*"

    val overallRptData = spark.sparkContext.textFile(vrPath).map(_.split(SPLIT_STR))
      .map(a => OverallRptData(a(0), a(1).toInt, a(2).toLong, a(3).toInt, a(4).toInt))
    val overallDF = spark.sqlContext.createDataFrame(overallRptData).toDF()
    overallDF.createOrReplaceTempView("overallRptData")
//    spark.udf.register("vehicleAverage", VehicleAverage)

    val selAllSql = "select accessId,powerStatus,dataTime,currentSpeed,currentMileage from overallRptData " +
      "order by accessId,dataTime "
    val allResults = spark.sql(selAllSql)

//    allResults.show(false)
//    allResults.printSchema
//    allResults.explain
//    allResults.dtypes.foreach(println)
//    allResults.columns.foreach(println)
//    allResults.head()

//    allResults.describe("accessId","dataTime","currentMileage").show(false)
//    allResults.where("powerStatus = 1" ).show(false)

//    allResults.selectExpr("substr(accessId,0,12)").show(false)     //配合udf使用
//    allResults.selectExpr("subtract(currentMileage)").show(false)
//    allResults.selectExpr("from_unixtime(dataTime,\"yyyy-MM-dd hh:mm:sss\")").show(false)

//    allResults.select($"accessId",$"currentMileage"+100).show(false)  //给currentMileage加100
//    allResults.filter("currentMileage>5000000").show(false)
//    allResults.filter($"currentMileage" > 5000000).show(false)
//    allResults.filter($"dataTime" > args(0)).filter($"dataTime" < args(1)).show(false)
//    allResults.sort($"accessId", $"dataTime".desc).show(false) //多字段排序
//    allResults.orderBy($"accessId", $"dataTime".desc).show(false)

    //分组：
    val ord = spark.table("overallRptData")
//    ord.groupBy("accessId").count.show
//    ord.groupBy($"accessId").avg().show   //所有的列求平均值
//    ord.groupBy($"accessId").avg("currentMileage").show   //currentMileage列求平均值
//    ord.groupBy($"accessId").agg("dataTime"->"max").show   //dataTime取最大
//    ord.groupBy($"accessId").agg("dataTime"->"min").show   //dataTime取最小
//    ord.groupBy($"accessId").agg("currentMileage"->"sum").show   //currentMileage求和
//    ord.groupBy($"accessId").agg("currentMileage"->"avg").show   //currentMileage求平均值
//    ord.groupBy($"accessId").pivot("currentMileage").agg(expr("coalesce(first(currentMileage),3)").cast("double")).show
//    ord.groupBy("accessId").pivot("powerStatus").count.show
//    ord.groupBy("accessId").mean("currentMileage").show

    //join：
//    val dept = hc.table("dept")
//    dept.show
//    emp.join(dept,emp.col("deptno") === dept.col("deptno"),"left_outer").show
//    emp.join(dept,emp.col("deptno") === dept.col("deptno"),"right_outer").show
//    emp.join(dept,emp.col("deptno") === dept.col("deptno"),"inner").show
//    emp.join(dept,$"emp.deptno"===$"dept.deptno" ,"inner").select("empno","ename","dname").show

//    val emp_dept = emp.join(dept,emp.col("deptno") === dept.col("deptno"),"left_outer")
//    emp_dept.registerTempTable("emp_dept_temp")
//    hc.sql("select count(*) from emp_dept_temp").collect

//    val allOrd = ord.where("powerStatus == 1").filter($"currentMileage" > 5000000).orderBy($"accessId", $"dataTime".desc)
//      .groupBy($"accessId").agg("dataTime"->"max","dataTime"->"min","currentMileage"->"avg")
//    allOrd.show(false)

    //	accessId  开始时间	结束时间 开始里程	结束里程	行驶总里程
//    val dataVehOverallSql = "select accessId,powerStatus,dataTime," +
//      "subtract(currentMileage) subtract_mileage from overallRptData " +
//      //"where powerStatus == 1 " +
//      "group by accessId,powerStatus,dataTime "
//    val vsResults = spark.sql(dataVehOverallSql)
//    vsResults.show(false)

//    spark.sql("SELECT COUNT(1) FROM hr.overallRptData te " +
//      "Left JOIN hr.overallRptData tr ON te.accessId = tr.accessId " +
//      "where tr.powerStatus == 1 ").show(false)

    spark.stop()
  }


}
