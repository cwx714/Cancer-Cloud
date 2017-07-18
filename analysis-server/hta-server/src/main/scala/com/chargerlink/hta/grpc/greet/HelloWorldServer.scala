//package com.chargerlink.hta.grpc.greet
//
//import io.grpc.Server
//import io.grpc.ServerBuilder
//import io.grpc.stub.StreamObserver
//import java.io.IOException
//import java.util.logging.Logger
//
//import io.grpc.ServerBuilder
//import io.grpc.stub.StreamObserver
//import java.io.IOException
//
//object HelloWorldServer {
//  private val logger = Logger.getLogger(classOf[HelloWorldServer].getName)
//
//  /**
//    * Main launches the server from the command line.
//    */
//  @throws[IOException]
//  @throws[InterruptedException]
//  def main(args: Array[String]): Unit = {
//    val server = new HelloWorldServer
//    server.start()
//    server.blockUntilShutdown()
//  }
//
//  class GreeterImpl extends Nothing {
//    def sayHello(req: Nothing, responseObserver: StreamObserver[Nothing]): Unit = {
//      val reply = HelloReply.newBuilder.setMessage("Hello " + req.getName).build
//      responseObserver.onNext(reply)
//      responseObserver.onCompleted()
//    }
//  }
//
//}
//
//class HelloWorldServer {
//  private var server = null
//
//  @throws[IOException]
//  private def start() = {
//    /* The port on which the server should run */ val port = 50051
//    server = ServerBuilder.forPort(port).addService(new HelloWorldServer.GreeterImpl).build.start
//    HelloWorldServer.logger.info("Server started, listening on " + port)
//    Runtime.getRuntime.addShutdownHook(new Thread() {
//      override def run(): Unit = { // Use stderr here since the logger may have been reset by its JVM shutdown hook.
//        System.err.println("*** shutting down gRPC server since JVM is shutting down")
//        thisHelloWorldServer.stop()
//        System.err.println("*** server shut down")
//      }
//    })
//  }
//
//  private def stop() = {
//    if (server != null) server.shutdown
//  }
//
//  /**
//    * Await termination on the main thread since the grpc library uses daemon threads.
//    */
//  @throws[InterruptedException]
//  private def blockUntilShutdown() = {
//    if (server != null) server.awaitTermination
//  }
//}
