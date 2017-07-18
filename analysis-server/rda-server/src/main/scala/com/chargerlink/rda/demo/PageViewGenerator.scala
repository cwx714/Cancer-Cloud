package com.chargerlink.rda.demo

import java.util.HashMap

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

import scala.util.Random

/**
  * Represents a page view on a website with associated dimension data.
  */
class PageView(val url : String, val status : Int, val zipCode : Int, val userID : Int)
    extends Serializable {
  override def toString() : String = {
    "%s\t%s\t%s\t%s\n".format(url, status, zipCode, userID)
  }
}

object PageView extends Serializable {
  def fromString(in : String) : PageView = {
    val parts = in.split("\t")
    if(parts.length < 3) {
      new PageView("http://foo.com/", 200, 94709, 7)
    } else {
      new PageView(parts(0), parts(1).toInt, parts(2).toInt, parts(3).toInt)
    }
  }
}

object PageViewGenerator {
  val pages = Map("http://foo.com/"        -> .7,
                  "http://foo.com/news"    -> 0.2,
                  "http://foo.com/contact" -> .1)
  val httpStatus = Map(200 -> .95,
                       404 -> .05)
  val userZipCode = Map(94709 -> .5,
                        94117 -> .5)
  val userID = Map((1 to 100).map(_ -> .01) : _*)

  def pickFromDistribution[T](inputMap : Map[T, Double]) : T = {
    val rand = new Random().nextDouble()
    var total = 0.0
    for ((item, prob) <- inputMap) {
      total = total + prob
      if (total > rand) {
        return item
      }
    }
    inputMap.take(1).head._1 // Shouldn't get here if probabilities add up to 1.0
  }

  def getNextClickEvent() : String = {
    val id = pickFromDistribution(userID)
    val page = pickFromDistribution(pages)
    val status = pickFromDistribution(httpStatus)
    val zipCode = pickFromDistribution(userZipCode)
    new PageView(page, status, zipCode, id).toString()
  }

  def main(args : Array[String]) {

    val broker = args(0)
    val topic = args(1)
//    val broker = "hdp21.cancer.com:6667"
//    val topic = "rda01"

    val port = 8080
    val viewsPerSecond = 1000
    val sleepDelayMs = (1000000.0 / viewsPerSecond).toInt

    val props = new HashMap[String, Object]()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)

    while (true) {
      new Thread() {
        override def run(): Unit = {
          while (true) {
            Thread.sleep(sleepDelayMs)
            print(getNextClickEvent())
            val message = new ProducerRecord[String, String](topic, null, getNextClickEvent())
            producer.send(message)
          }
        }
      }.start()
    }
  }
}
