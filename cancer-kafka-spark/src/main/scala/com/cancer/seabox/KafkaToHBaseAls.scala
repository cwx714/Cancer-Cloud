package com.cancer.seabox

import kafka.serializer.StringDecoder

import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.io.ImmutableBytesWritable

import scala.util.Random

object KafkaToHBaseAls extends Serializable {
  final val zookeeperQuorum = "host2,host3,host1,host4,host5" // "host2,host3,host4" // "dfjx71,dfjx72,dfjx73"
  final val tableName = "cf01"
  final val cfFamilyBytes = Bytes.toBytes("info")
  final val colMovieBytes = Bytes.toBytes("movie")
  final val colUserBytes = Bytes.toBytes("user")
  final val colFeatureBytes = Bytes.toBytes("feature")
  final val broker = "host2:6667" //"dfjx71:9092" "host2:9092"
  final val topics = Set("dfjx-cf01")

  def main(args: Array[String]) {
    val master = args.length match {
      case x: Int if x > 0 => args(0)
      case _ => "local"
    }
    val sparkConf = new SparkConf().setAppName("KafkaToHBaseAls").setMaster(master)
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val kafkaParams = Map[String, String]("metadata.broker.list" -> broker)
    val kafkaDstreams = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
        ssc, kafkaParams, topics)
    val movieLensRDD = kafkaDstreams.map(_._2).flatMap(_.split("\n"))
    val standRDD = movieLensRDD.map(line => {
      if (!line.contains("::")) {
        println("This data is not standardized: " + line)
      } else {
        persistHBase(line)
        println("This data is standardized: " + line)
      }
    }).print()

    /*val movieLensRDD = kafkaDstreams.map(_._2).flatMap(_.split("\n")).map(MovieLens.fromString(_))
    movieLensRDD.map(ml => {
      val rand = new Random().nextInt((999999 - 100000) + 1)
      val row = Bytes.toBytes("cf" + rand)
      val p = new Put(row)
      p.addColumn(cfDataBytes, colMovieBytes, Bytes.toBytes(ml.movie))
      p.addColumn(cfDataBytes, colUserBytes, Bytes.toBytes(ml.user))
      p.addColumn(cfDataBytes, colFeatureBytes, Bytes.toBytes(ml.feature))
      table.put(p)

//      convertToPut(ml).toString()
    }).print()*/

    ssc.start()
    ssc.awaitTermination()
  }

  def persistHBase(line : String): Unit = {
    // set up HBase Table configuration
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", zookeeperQuorum)
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set("hbase.defaults.for.version.skip", "true")
        conf.set("zookeeper.znode.parent", "/hbase-unsecure")
    conf.set(TableInputFormat.INPUT_TABLE, tableName)

    val table = new HTable(conf, tableName)

    val parts = line.split("::")
    val rand = new Random().nextInt((999999 - 100000) + 1)
    val row = Bytes.toBytes("cf" + rand)
    val p = new Put(row)
    p.addColumn(cfFamilyBytes, colMovieBytes, Bytes.toBytes(parts(0).toInt))
    p.addColumn(cfFamilyBytes, colUserBytes, Bytes.toBytes(parts(1).toInt))
    p.addColumn(cfFamilyBytes, colFeatureBytes, Bytes.toBytes(parts(2).toInt))

    table.put(p)
  }

  def convertToPut(movieLens: MovieLens): (ImmutableBytesWritable, Put)  = {
    val rand = new Random().nextInt((999999 - 100000) + 1)
    val row = Bytes.toBytes("cf" + rand)
    val p = new Put(row)
    // add to column family data, column  data values to put object
    p.addColumn(cfFamilyBytes, colMovieBytes, Bytes.toBytes(movieLens.movie))
    p.addColumn(cfFamilyBytes, colUserBytes, Bytes.toBytes(movieLens.user))
    p.addColumn(cfFamilyBytes, colFeatureBytes, Bytes.toBytes(movieLens.feature))
    return (new ImmutableBytesWritable(row), p)
  }
}

class MovieLens(val movie : Int, val user : Int, val feature : Int)
  extends Serializable {
  override def toString() : String = {
    "%s\t%s\t%s\t%s\n".format(movie, user, feature)
  }
}

object MovieLens extends Serializable {
  def fromString(in : String) : MovieLens = {
    val parts = in.split("::")
    new MovieLens(parts(0).toInt, parts(1).toInt, parts(2).toInt)
  }
}
