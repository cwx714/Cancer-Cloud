package com.chargerlink.hta.report

import org.apache.spark.sql.SparkSession

/**
  * Created by Cuiwx on 2017/7/12/012.
  */
object OverallAnalyzer {
  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("OverallAnalyzer").master("local[*]").getOrCreate()
    val sc = spark.sparkContext
    val sqlContext = spark.sqlContext
    sc.setLogLevel("WARN")

    val SPLIT_STR = "-"
    val vrPath = args(0)
//    val vrPath = "file:/D:/Work/Code/analysis-server/hta-server/data/rda-vr.txt"
    val vrData = sc.textFile(vrPath)

    //accessId,powerStatus,dataTime,currentSpeed,currentMileage
    val vrRDD = vrData.map(_.split(SPLIT_STR)).map(vr => (vr(0), vr(1), vr(2), vr(3), vr(4)))//.cache()

//    val vrRDDRs = vrRDD.reduce((x,y) => (x._1, x._2, x._3, x._4, (y._5.toInt - x._5.toInt).toString))
//    println(vrRDDRs._5)

    val diffMileageRDD = vrRDD.map(x => (x._1, x._5.toInt)).groupByKey().map(f => {
      var fmax = f._2.max
      var fmin = f._2.min
      (f._1,fmax - fmin)
    }).filter(p => (p._2 > 0))

//    println("==============================")

    val vrNoRDD = vrRDD.map(x => (x._1, x._2.toInt)).groupByKey().map(f => {

      var vrNo = f._2.iterator.next()
//      if(f._2.nonEmpty) vrNo = f._2.iterator.next()
      if(!f._2.iterator.next().equals(vrNo)) vrNo += 1
//      for (elem <- f._2) {
//        //println(f._1 + "@@@" + elem)
//        vrNo = elem
//      }
      (f._1,(vrNo,f._2))
    }).foreach(println)


    val accIdRDD = vrRDD.map(x => (x._1,(x._2.toInt, x._3.toLong, x._4.toInt, x._5.toInt)))
      .groupByKey()
      .map(t => {
        t._2.map(f => {
          var vrNo = 1
          if(f._1.toInt == 1) vrNo += 1
          (vrNo, f._1.toInt, f._2.toLong, f._3.toInt, f._4.toInt)
        }).filter(p => (p._2 == 1))
        t
      })

//    accIdRDD.join(diffMileageRDD).foreach(println)
  }
}
