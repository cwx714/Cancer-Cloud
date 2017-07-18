package com.chargerlink.hta.grpc

import java.text.SimpleDateFormat
import java.util.concurrent.TimeUnit
import java.util.logging.{Level, Logger}

import com.chargerlink.gateway.analyser
import com.chargerlink.gateway.analyser.{QueryServiceGrpc, ReportQueryReq, ReportQueryResp}
import io.grpc.{ManagedChannel, ManagedChannelBuilder, StatusRuntimeException}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

/**
  * Created by Cuiwx on 2017/7/6/006.
  */
object GrpcClient {
  val logger = Logger.getLogger(classOf[GrpcClient].getName)

  @throws[Exception]
  def main(args: Array[String]): Unit = {
    /* Access a service running on the local machine on port 50051 */
    val host = args(0)
    val port = args(1).toInt
    val client = new GrpcClient(host, port)
      try {
        val req = ReportQueryReq(1,System.currentTimeMillis(), System.currentTimeMillis(),ReportQueryReq.Period.PERIOD_ALL )
        client.queryFunc(req)
    } finally client.shutdown()
  }
}

class GrpcClient(val channel: ManagedChannel) {

  final private var blockingStub = QueryServiceGrpc.blockingStub(channel)

  def this(host: String, port: Int) {
    // Channels are secure by default (via SSL/TLS). For the example we disable TLS to avoid needing certificates.
    this(ManagedChannelBuilder.forAddress(host, port).usePlaintext(true).build)
  }

  @throws[InterruptedException]
  def shutdown(): Unit = {
    channel.shutdown.awaitTermination(5, TimeUnit.SECONDS)
  }

  def queryFunc(req: ReportQueryReq): Unit = {
    try{
      GrpcClient.logger.log(Level.WARNING, s"req is $req")
      val stub = QueryServiceGrpc.stub(channel)
      val response = stub.query(req)
//      val response = blockingStub.query(req)
      GrpcClient.logger.log(Level.WARNING, s"response is $response")
    }
    catch {
      case e: StatusRuntimeException =>
        GrpcClient.logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus)
    }
//    GrpcClient.logger.info("Result: " + response.records.toString())
  }

}