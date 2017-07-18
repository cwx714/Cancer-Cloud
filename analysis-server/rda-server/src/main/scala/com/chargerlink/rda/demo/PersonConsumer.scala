/*
package com.chargerlink.rda.alert

import org.apache.spark.rdd.RDD
import com.trueaccord.scalapb.spark._
import org.apache.spark.sql.{SQLContext, SparkSession}
import com.example.protos.demo._

object PersonConsumer {
  def main(args : Array[String]) {
    def parseLine(s: String): Person =
      Person.parseFrom(
        org.apache.commons.codec.binary.Base64.decodeBase64(s))

    val spark = SparkSession.builder.
      master("local")
      .appName("spark session example")
      .getOrCreate()

    import spark.implicits._

    val ds1 = spark.readStream.format("kafka").option("kafka.bootstrap.servers","localhost:9092").option("subscribe","person").load()
    val ds2 = ds1.selectExpr("CAST(value AS STRING)").as[String]
    val ds3 = ds2.map(str => parseLine(str))
    sqlContext.protoToDataFrame(ds3).registerTempTable("persons")
    val ds4 = spark.sqlContext.sql("select name from persons")
    val query = ds4.writeStream
      .outputMode("append")
      .format("console")
      .start()
    query.awaitTermination()
  }
}
*/
