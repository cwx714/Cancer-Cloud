//package com.chargerlink.hta.grpc.greet
//
//import com.chargerlink.hta.grpc.greet.HelloWorldServer
//
//import java.util.logging.Level
//import java.util.logging.Logger
//
//import io.grpc.ManagedChannel
//import io.grpc.ManagedChannelBuilder
//import io.grpc.StatusRuntimeException
//import java.util.concurrent.TimeUnit
//
///**
//  * A simple client that requests a greeting from the {@link HelloWorldServer}.
//  */
//object HelloWorldClient {
//  private val logger = Logger.getLogger(classOf[HelloWorldClient].getName)
//
//  /**
//    * Greet server. If provided, the first element of {@code args} is the name to use in the
//    * greeting.
//    */
//  @throws[Exception]
//  def main(args: Array[String]): Unit = {
//    val client = new HelloWorldClient("localhost", 50051)
//    try {
//      /* Access a service running on the local machine on port 50051 */ var user = "world"
//      if (args.length > 0) user = args(0) /* Use the arg as the name to greet if provided */
//      client.greet(user)
//    } finally client.shutdown()
//  }
//}
//
//class HelloWorldClient(val channel: ManagedChannel)
//
///** Construct client for accessing RouteGuide server using the existing channel. */ {
//  blockingStub = GreeterGrpc.newBlockingStub(channel)
//  final private var blockingStub = null
//
//  /** Construct client connecting to HelloWorld server at {@code host:port}. */
//  def this(host: String, port: Int) {
//    this(ManagedChannelBuilder.forAddress(host, port).usePlaintext // Channels are secure by default (via SSL/TLS). For the example we disable TLS to avoid
//      // needing certificates.
//      (true).build)
//  }
//
//  @throws[InterruptedException]
//  def shutdown(): Unit = {
//    channel.shutdown.awaitTermination(5, TimeUnit.SECONDS)
//  }
//
//  /** Say hello to server. */
//  def greet(name: String): Unit = {
//    HelloWorldClient.logger.info("Will try to greet " + name + " ...")
//    val request = HelloRequest.newBuilder.setName(name).build
//    var response = null
//    try
//      response = blockingStub.sayHello(request)
//    catch {
//      case e: StatusRuntimeException =>
//        HelloWorldClient.logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus)
//        return
//    }
//    HelloWorldClient.logger.info("Greeting: " + response.getMessage)
//  }
//}
