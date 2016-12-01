package com.cancer.seabox

import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.client.Result
import org.apache.spark._
import org.apache.spark.util.StatCounter

import scala.util.Random

object CFHBaseOps extends Serializable {

  final val zookeeperQuorum = "host2,host3,host1,host4,host5"
  final val cfFamilyBytes = Bytes.toBytes("info")
  final val colMovieBytes = Bytes.toBytes("movie")
  final val colUserBytes = Bytes.toBytes("user")
  final val colFeatureBytes = Bytes.toBytes("feature")

  def main(args: Array[String]) {
    val master = args.length match {
      case x: Int if x > 0 => args(0)
      case _ => "local"
    }
    val sparkConf = new SparkConf().setAppName("CFHBaseOps").setMaster(master)
    val sc = new SparkContext(sparkConf)

    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", zookeeperQuorum)
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set("hbase.defaults.for.version.skip", "true")
    conf.set("zookeeper.znode.parent", "/hbase-unsecure")

    val conn = ConnectionFactory.createConnection(conf)
    val admin = conn.getAdmin

    val tabname = TableName.valueOf("cf01")
    if(admin.tableExists(tabname)){
      admin.disableTable(tabname)
      admin.deleteTable(tabname)
    }

    val htd = new HTableDescriptor(tabname)
    val hcd = new HColumnDescriptor("info")
    //add  column to table
    htd.addFamily(hcd)
    admin.createTable(htd)

    //put data to HBase table
    val htdTablename = htd.getName
    val table = new HTable(conf, htdTablename)
    val dataBytes = Bytes.toBytes("info")

    for (c <- 1 to 10) {
      val rand = new Random().nextInt((999999 - 100000) + 1)
      val row = Bytes.toBytes("cf" + rand)
      val p = new Put(row)
      p.addColumn(dataBytes, Bytes.toBytes("movie"), Bytes.toBytes("10" + rand))
      p.addColumn(dataBytes, Bytes.toBytes("user"), Bytes.toBytes("1" + rand))
      p.addColumn(dataBytes, Bytes.toBytes("feature"), Bytes.toBytes("11" + rand))
      table.put(p)
    }

    //search table
    conf.set(TableInputFormat.INPUT_TABLE, "cf01")
    val hbaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    val count = hbaseRDD.count()
    println("HbaseRDD Count:" + count)
    hbaseRDD.cache()

    val resultRDD = hbaseRDD.map(tuple => tuple._2)
    println("resultRDD.count() :: " + resultRDD.count())

    val keyValueRDD = resultRDD.map(result => (Bytes.toString(result.getRow()), Bytes.toInt(result.value)))
    keyValueRDD.foreach(
      f => println(f._1 + "***" + f._2)
    )
    println("keyValueRDD.first() :: " + keyValueRDD.first()._1 + "---" + keyValueRDD.first()._2)

    val keyStatsRDD = keyValueRDD.groupByKey().mapValues(list => list.take(10))
    keyStatsRDD.foreach(
      f => println(f._1 + "+++" + f._2)
    )

    val cfFamilyBytes = Bytes.toBytes("info")
    val colMovieBytes = Bytes.toBytes("movie")
    val colUserBytes = Bytes.toBytes("user")
    val colFeatureBytes = Bytes.toBytes("feature")
    val strRDD = hbaseRDD.map[String](
      (tup:(ImmutableBytesWritable, Result)) =>
        new String( Bytes.toInt(tup._2.getValue(cfFamilyBytes, colMovieBytes))
          + "::" + Bytes.toInt(tup._2.getValue(cfFamilyBytes, colUserBytes))
          + "::" + Bytes.toInt(tup._2.getValue(cfFamilyBytes, colFeatureBytes))
        )
     )
    strRDD.collect().foreach(println)

    /*val res = hbaseRDD.take(count.toInt)
    for (j <- 1 to count.toInt) {
      var rs = res(j - 1)._2

      var kvs = rs.raw
      for (kv <- kvs)
        println("row:" + new String(kv.getRow()) +
          " cf:" + new String(kv.getFamily()) +
          " column:" + new String(kv.getQualifier()) +
          " value:" + new String(kv.getValue()))
    }*/

  }
}
