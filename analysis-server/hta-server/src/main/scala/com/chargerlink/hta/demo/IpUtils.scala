package com.chargerlink.hta.demo

/**
  * Created by Cuiwx on 2017/7/13/013.
  */
object IpUtils {
  def ipExistsInRange(ip: String, ipSection: String):Boolean = {
    val idx = ipSection.indexOf('-')
    val beginIp = ipSection.substring(0, idx)
    val endIp = ipSection.substring(idx + 1)
    return getIp2long(beginIp) <= getIp2long(ip) && getIp2long(ip) <= getIp2long(endIp)
  }

  def getIp2long(ip: String): Long = {
    var ips = ip.split("\\.");
    var ip2long = 0L;
    for (i <- 1 to 10) {
      //      ip2long = ip2long << 8 | ips(i).toInt//Integer.parseInt(ips[i])
      ip2long = ip2long << 8 | ips(i).toInt
    }
    ip2long
  }
}
