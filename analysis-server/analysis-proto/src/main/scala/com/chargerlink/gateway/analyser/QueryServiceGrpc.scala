package com.chargerlink.gateway.analyser

object QueryServiceGrpc {
  val METHOD_QUERY: _root_.io.grpc.MethodDescriptor[com.chargerlink.gateway.analyser.ReportQueryReq, com.chargerlink.gateway.analyser.ReportQueryResp] =
    _root_.io.grpc.MethodDescriptor.create(
      _root_.io.grpc.MethodDescriptor.MethodType.UNARY,
      _root_.io.grpc.MethodDescriptor.generateFullMethodName("com.chargerlink.gateway.analyser.QueryService", "Query"),
      new com.trueaccord.scalapb.grpc.Marshaller(com.chargerlink.gateway.analyser.ReportQueryReq),
      new com.trueaccord.scalapb.grpc.Marshaller(com.chargerlink.gateway.analyser.ReportQueryResp))
  
  val METHOD_SUBSCRIBE: _root_.io.grpc.MethodDescriptor[com.chargerlink.gateway.analyser.ReportQueryReq, com.chargerlink.gateway.analyser.ReportQueryResp] =
    _root_.io.grpc.MethodDescriptor.create(
      _root_.io.grpc.MethodDescriptor.MethodType.BIDI_STREAMING,
      _root_.io.grpc.MethodDescriptor.generateFullMethodName("com.chargerlink.gateway.analyser.QueryService", "Subscribe"),
      new com.trueaccord.scalapb.grpc.Marshaller(com.chargerlink.gateway.analyser.ReportQueryReq),
      new com.trueaccord.scalapb.grpc.Marshaller(com.chargerlink.gateway.analyser.ReportQueryResp))
  
  trait QueryService extends _root_.com.trueaccord.scalapb.grpc.AbstractService {
    override def serviceCompanion = QueryService
    def query(request: com.chargerlink.gateway.analyser.ReportQueryReq): scala.concurrent.Future[com.chargerlink.gateway.analyser.ReportQueryResp]
    def subscribe(responseObserver: _root_.io.grpc.stub.StreamObserver[com.chargerlink.gateway.analyser.ReportQueryResp]): _root_.io.grpc.stub.StreamObserver[com.chargerlink.gateway.analyser.ReportQueryReq]
  }
  
  object QueryService extends _root_.com.trueaccord.scalapb.grpc.ServiceCompanion[QueryService] {
    implicit def serviceCompanion: _root_.com.trueaccord.scalapb.grpc.ServiceCompanion[QueryService] = this
    def javaDescriptor: _root_.com.google.protobuf.Descriptors.ServiceDescriptor = com.chargerlink.gateway.analyser.QueryProto.javaDescriptor.getServices().get(0)
  }
  
  trait QueryServiceBlockingClient {
    def serviceCompanion = QueryService
    def query(request: com.chargerlink.gateway.analyser.ReportQueryReq): com.chargerlink.gateway.analyser.ReportQueryResp
  }
  
  class QueryServiceBlockingStub(channel: _root_.io.grpc.Channel, options: _root_.io.grpc.CallOptions = _root_.io.grpc.CallOptions.DEFAULT) extends _root_.io.grpc.stub.AbstractStub[QueryServiceBlockingStub](channel, options) with QueryServiceBlockingClient {
    override def query(request: com.chargerlink.gateway.analyser.ReportQueryReq): com.chargerlink.gateway.analyser.ReportQueryResp = {
      _root_.io.grpc.stub.ClientCalls.blockingUnaryCall(channel.newCall(METHOD_QUERY, options), request)
    }
    
    override def build(channel: _root_.io.grpc.Channel, options: _root_.io.grpc.CallOptions): QueryServiceBlockingStub = new QueryServiceBlockingStub(channel, options)
  }
  
  class QueryServiceStub(channel: _root_.io.grpc.Channel, options: _root_.io.grpc.CallOptions = _root_.io.grpc.CallOptions.DEFAULT) extends _root_.io.grpc.stub.AbstractStub[QueryServiceStub](channel, options) with QueryService {
    override def query(request: com.chargerlink.gateway.analyser.ReportQueryReq): scala.concurrent.Future[com.chargerlink.gateway.analyser.ReportQueryResp] = {
      com.trueaccord.scalapb.grpc.Grpc.guavaFuture2ScalaFuture(_root_.io.grpc.stub.ClientCalls.futureUnaryCall(channel.newCall(METHOD_QUERY, options), request))
    }
    
    override def subscribe(responseObserver: _root_.io.grpc.stub.StreamObserver[com.chargerlink.gateway.analyser.ReportQueryResp]): _root_.io.grpc.stub.StreamObserver[com.chargerlink.gateway.analyser.ReportQueryReq] = {
      _root_.io.grpc.stub.ClientCalls.asyncBidiStreamingCall(channel.newCall(METHOD_SUBSCRIBE, options), responseObserver)
    }
    
    override def build(channel: _root_.io.grpc.Channel, options: _root_.io.grpc.CallOptions): QueryServiceStub = new QueryServiceStub(channel, options)
  }
  
  def bindService(serviceImpl: QueryService, executionContext: scala.concurrent.ExecutionContext): _root_.io.grpc.ServerServiceDefinition =
    _root_.io.grpc.ServerServiceDefinition.builder("com.chargerlink.gateway.analyser.QueryService")
    .addMethod(
      METHOD_QUERY,
      _root_.io.grpc.stub.ServerCalls.asyncUnaryCall(new _root_.io.grpc.stub.ServerCalls.UnaryMethod[com.chargerlink.gateway.analyser.ReportQueryReq, com.chargerlink.gateway.analyser.ReportQueryResp] {
        override def invoke(request: com.chargerlink.gateway.analyser.ReportQueryReq, observer: _root_.io.grpc.stub.StreamObserver[com.chargerlink.gateway.analyser.ReportQueryResp]): Unit =
          serviceImpl.query(request).onComplete(com.trueaccord.scalapb.grpc.Grpc.completeObserver(observer))(
            executionContext)
      }))
    .addMethod(
      METHOD_SUBSCRIBE,
      _root_.io.grpc.stub.ServerCalls.asyncBidiStreamingCall(new _root_.io.grpc.stub.ServerCalls.BidiStreamingMethod[com.chargerlink.gateway.analyser.ReportQueryReq, com.chargerlink.gateway.analyser.ReportQueryResp] {
        override def invoke(observer: _root_.io.grpc.stub.StreamObserver[com.chargerlink.gateway.analyser.ReportQueryResp]): _root_.io.grpc.stub.StreamObserver[com.chargerlink.gateway.analyser.ReportQueryReq] =
          serviceImpl.subscribe(observer)
      }))
    .build()
  
  def blockingStub(channel: _root_.io.grpc.Channel): QueryServiceBlockingStub = new QueryServiceBlockingStub(channel)
  
  def stub(channel: _root_.io.grpc.Channel): QueryServiceStub = new QueryServiceStub(channel)
  
  def javaDescriptor: _root_.com.google.protobuf.Descriptors.ServiceDescriptor = com.chargerlink.gateway.analyser.QueryProto.javaDescriptor.getServices().get(0)
  
}