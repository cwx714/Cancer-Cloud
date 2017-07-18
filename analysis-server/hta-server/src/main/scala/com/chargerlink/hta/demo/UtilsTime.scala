//package com.chargerlink.hta.demo
//
//import Java.text.SimpleDateFormat
//import java.sql.Timestamp
//import java.sql.Date
//import org.apache.spark.sql.catalyst.expressions.CurrentDate
//import org.apache.spark.sql.catalyst.util.DateTimeUtils
//
///**
//  * Created by Cuiwx on 2017/7/13/013.
//  */
//object UtilsTime {
//  def main(args: Array[String]): Unit = {
//
//    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
//    val sdfDate = new SimpleDateFormat("yyyy-MM-dd")
//    val d = new Date(sdf.parse("2015-04-08 13:10:15").getTime)
//    val ts = new Timestamp(sdf.parse("2013-11-08 13:10:15").getTime)
//
//    println(d)
//    println(ts)
//
//    val startTime = "2016-11-1 00:00:00"
//    val endTime = "2016-11-1 23:59:59"
//
//    val test = "$'time' >= '" + startTime + "' and $'time' <= '" + endTime + "'"
//    println(test)
//    val imfd = new Date(sdf.parse(endTime).getTime)
//    val imfts = new Timestamp(sdf.parse(endTime).getTime)
//
//    println(imfd)
//    println(imfts)
//
//    val d0 = DateTimeUtils.millisToDays(System.currentTimeMillis())
//    val d1 = DateTimeUtils.millisToDays(System.currentTimeMillis())
//    println(d0)
//    println(d1)
//
//  }
//}
