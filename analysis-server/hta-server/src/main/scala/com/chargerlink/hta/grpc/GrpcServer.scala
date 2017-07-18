package com.chargerlink.hta.grpc

import com.chargerlink.gateway.analyser._
import java.text.SimpleDateFormat
import java.util.logging.{Level, Logger}

import io.grpc.{Server, ServerBuilder}
import io.grpc.stub.StreamObserver
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.concurrent.{ExecutionContext, Future}

/**
  * Author gebing
  * Date: 2017/07/05
  */
object GrpcServer {
  private var port = 50051

  def main(args: Array[String]): Unit = {
    val server = new GrpcServer(ExecutionContext.global)
    server.start()
    server.blockUntilShutdown()
  }
}

class GrpcServer(executionContext: ExecutionContext) { self =>
  private[this] val logger = Logger.getLogger(classOf[GrpcServer].getName)
  private[this] var server: Server = null

  private def start(): Unit = {
    server = ServerBuilder.forPort(GrpcServer.port).addService(QueryServiceGrpc.bindService(new QueryServiceImpl, executionContext)).build.start
    logger.info("Server started, listening on " + GrpcServer.port)
    sys.addShutdownHook {
      System.err.println("*** shutting down gRPC server since JVM is shutting down")
      self.stop()
      System.err.println("*** server shut down")
    }
  }

  private def stop(): Unit = {
    if (server != null) {
      server.shutdown()
    }
  }

  private def blockUntilShutdown(): Unit = {
    if (server != null) {
      server.awaitTermination()
    }
  }

  var Unit1: Unit = _

  private class QueryServiceImpl extends QueryServiceGrpc.QueryService {

    override def query(request: ReportQueryReq): scala.concurrent.Future[ReportQueryResp] = {
      logger.log(Level.WARNING, s"request is $request")
      val reply: ReportQueryResp = queryUseSparkSQL(request)
      logger.log(Level.WARNING, s"reply is $reply")
      Future.successful(reply)
    }

    override def subscribe(responseObserver: StreamObserver[ReportQueryResp]): StreamObserver[ReportQueryReq] = {
      throw new Exception("Not implement!")
    }

    /**
      * Query Use SparkSQL
      *
      * @param request
      * @return
      */
    def queryUseSparkSQL(request: ReportQueryReq): ReportQueryResp = {
      println(request.reportId + " -+- " + request.params)
      val resp = ReportQueryResp.defaultInstance

//      val spark = SparkSession.builder()
//        .appName("Vehicle Record Use GrpcServer")
//        .master("local[*]")//部署生产环境时注释
//        .getOrCreate()
//      spark.sparkContext.setLogLevel("WARN")
//
//      val SPLIT_STR = "-" //"\t"
//      val DATE_PARTTEN = "yyyy-MM-dd HH:mm"
//      val odPath = "/user/spark/rda/vehicle/*/*"
//      val overallData = spark.sparkContext.textFile(odPath).map(_.split(SPLIT_STR))
//        .map(a => VehicleRecordGrpc(a(0), a(1), new SimpleDateFormat(DATE_PARTTEN).format(a(2).toLong), a(3).toInt, a(4).toInt))
//
//      val overallDF = spark.sqlContext.createDataFrame(overallData).toDF()
//      overallDF.createOrReplaceTempView("vehicleRecord")
//
//      //	accessId  开始时间	结束时间 开始里程	结束里程	行驶总里程
//      val dataVehOverallSql = "select accessId," +
//        "min(dataTime) start_time,max(dataTime) stop_time," +
//        "min(currentMileage) start_mileage,max(currentMileage) stop_mileage," +
//        "sum(currentMileage) total_mileage from vehicleRecord group by accessId,recordNo "
//      val vsResults = spark.sql(dataVehOverallSql)
//      vsResults.show(false)
//      //val rs = vsResults.repartition(1).write.mode(SaveMode.Append).format("csv")
//
//      vsResults.foreach(f => {
//        val rs = f(0) + SPLIT_STR + f(1) + SPLIT_STR + f(2) + SPLIT_STR + f(3) + SPLIT_STR + f(4) + SPLIT_STR + SPLIT_STR + f(5)
//        println("rs == " + rs)
//        resp.records.addString(new StringBuilder(rs))
//      })

      resp.records.addString(new StringBuilder("4096:LNJS6543223453456-1-1498199256000-959-4054840"))
      resp.records.addString(new StringBuilder("4096:LNJS6543223453456-1-1498199326000-1728-7554345"))
      resp.records.addString(new StringBuilder("4096:LNJS6543223453456-2-1498200456000-1215-126584"))
      resp.records.addString(new StringBuilder("4096:LNJS6543223453456-2-1498200956000-1215-126584"))
      resp.records.addString(new StringBuilder("4096:LNJS6543223453457-1-1498717690000-595-2110178"))
      resp.records.addString(new StringBuilder("4096:LNJS6543223453457-2-1498198356000-1684-4342763"))

      println("resp.records == " + resp.records)

      return resp
    }
  }

//  private class RulesServiceImpl extends RulesServiceGrpc.RulesService {
//    override def setAlarmRule(request: AlarmRule): Future[Result] = ???
//
//    override def removeAlarmRule(request: AlarmRule): Future[Result] = ???
//  }
}

case class VehicleRecordGrpc(accessId:String, recordNo:String, dataTime:String, currentSpeed:Int, currentMileage:Int)