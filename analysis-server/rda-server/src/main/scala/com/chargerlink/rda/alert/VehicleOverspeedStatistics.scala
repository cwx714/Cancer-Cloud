package com.chargerlink.rda.alert

import org.joda.time._
import org.joda.time.format._
import java.text.SimpleDateFormat
/**
  * Created by Cuiwx on 2017/6/16/016.
  */
object VehicleOverspeedStatistics {
  def main(args: Array[String]): Unit = {

    val mybrithday = "2017-07-14 12:34:56"
    val timeFromStr = DateTime.parse(mybrithday,DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:sss"))
    println(timeFromStr)

    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    println(sdf.parse(mybrithday))
    println(sdf.format("1499000015000".toLong))

    val DATE_PARTTEN = "yyyy-MM-dd HH:mm" //"yyyy-MM-dd HH:mm:ss"
    val format =  new SimpleDateFormat(DATE_PARTTEN)
    val time = "1498197270000".toLong // 1498197965000
    val d = format.format(time)
//    val date = format.parse(d)
    println(d)
//    println(date)

    val timeLength = "1501000075000".toLong // 6489374 //1499000075000
    val len = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(timeLength)
    println(len)
  }
}
