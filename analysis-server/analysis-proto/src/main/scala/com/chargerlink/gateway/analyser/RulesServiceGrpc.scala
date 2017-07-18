package com.chargerlink.gateway.analyser

object RulesServiceGrpc {
  val METHOD_SET_ALARM_RULE: _root_.io.grpc.MethodDescriptor[com.chargerlink.gateway.analyser.AlarmRule, com.chargerlink.gateway.analyser.Result] =
    _root_.io.grpc.MethodDescriptor.create(
      _root_.io.grpc.MethodDescriptor.MethodType.UNARY,
      _root_.io.grpc.MethodDescriptor.generateFullMethodName("com.chargerlink.gateway.analyser.RulesService", "SetAlarmRule"),
      new com.trueaccord.scalapb.grpc.Marshaller(com.chargerlink.gateway.analyser.AlarmRule),
      new com.trueaccord.scalapb.grpc.Marshaller(com.chargerlink.gateway.analyser.Result))
  
  val METHOD_REMOVE_ALARM_RULE: _root_.io.grpc.MethodDescriptor[com.chargerlink.gateway.analyser.AlarmRule, com.chargerlink.gateway.analyser.Result] =
    _root_.io.grpc.MethodDescriptor.create(
      _root_.io.grpc.MethodDescriptor.MethodType.UNARY,
      _root_.io.grpc.MethodDescriptor.generateFullMethodName("com.chargerlink.gateway.analyser.RulesService", "RemoveAlarmRule"),
      new com.trueaccord.scalapb.grpc.Marshaller(com.chargerlink.gateway.analyser.AlarmRule),
      new com.trueaccord.scalapb.grpc.Marshaller(com.chargerlink.gateway.analyser.Result))
  
  trait RulesService extends _root_.com.trueaccord.scalapb.grpc.AbstractService {
    override def serviceCompanion = RulesService
    def setAlarmRule(request: com.chargerlink.gateway.analyser.AlarmRule): scala.concurrent.Future[com.chargerlink.gateway.analyser.Result]
    def removeAlarmRule(request: com.chargerlink.gateway.analyser.AlarmRule): scala.concurrent.Future[com.chargerlink.gateway.analyser.Result]
  }
  
  object RulesService extends _root_.com.trueaccord.scalapb.grpc.ServiceCompanion[RulesService] {
    implicit def serviceCompanion: _root_.com.trueaccord.scalapb.grpc.ServiceCompanion[RulesService] = this
    def javaDescriptor: _root_.com.google.protobuf.Descriptors.ServiceDescriptor = com.chargerlink.gateway.analyser.RulesProto.javaDescriptor.getServices().get(0)
  }
  
  trait RulesServiceBlockingClient {
    def serviceCompanion = RulesService
    def setAlarmRule(request: com.chargerlink.gateway.analyser.AlarmRule): com.chargerlink.gateway.analyser.Result
    def removeAlarmRule(request: com.chargerlink.gateway.analyser.AlarmRule): com.chargerlink.gateway.analyser.Result
  }
  
  class RulesServiceBlockingStub(channel: _root_.io.grpc.Channel, options: _root_.io.grpc.CallOptions = _root_.io.grpc.CallOptions.DEFAULT) extends _root_.io.grpc.stub.AbstractStub[RulesServiceBlockingStub](channel, options) with RulesServiceBlockingClient {
    override def setAlarmRule(request: com.chargerlink.gateway.analyser.AlarmRule): com.chargerlink.gateway.analyser.Result = {
      _root_.io.grpc.stub.ClientCalls.blockingUnaryCall(channel.newCall(METHOD_SET_ALARM_RULE, options), request)
    }
    
    override def removeAlarmRule(request: com.chargerlink.gateway.analyser.AlarmRule): com.chargerlink.gateway.analyser.Result = {
      _root_.io.grpc.stub.ClientCalls.blockingUnaryCall(channel.newCall(METHOD_REMOVE_ALARM_RULE, options), request)
    }
    
    override def build(channel: _root_.io.grpc.Channel, options: _root_.io.grpc.CallOptions): RulesServiceBlockingStub = new RulesServiceBlockingStub(channel, options)
  }
  
  class RulesServiceStub(channel: _root_.io.grpc.Channel, options: _root_.io.grpc.CallOptions = _root_.io.grpc.CallOptions.DEFAULT) extends _root_.io.grpc.stub.AbstractStub[RulesServiceStub](channel, options) with RulesService {
    override def setAlarmRule(request: com.chargerlink.gateway.analyser.AlarmRule): scala.concurrent.Future[com.chargerlink.gateway.analyser.Result] = {
      com.trueaccord.scalapb.grpc.Grpc.guavaFuture2ScalaFuture(_root_.io.grpc.stub.ClientCalls.futureUnaryCall(channel.newCall(METHOD_SET_ALARM_RULE, options), request))
    }
    
    override def removeAlarmRule(request: com.chargerlink.gateway.analyser.AlarmRule): scala.concurrent.Future[com.chargerlink.gateway.analyser.Result] = {
      com.trueaccord.scalapb.grpc.Grpc.guavaFuture2ScalaFuture(_root_.io.grpc.stub.ClientCalls.futureUnaryCall(channel.newCall(METHOD_REMOVE_ALARM_RULE, options), request))
    }
    
    override def build(channel: _root_.io.grpc.Channel, options: _root_.io.grpc.CallOptions): RulesServiceStub = new RulesServiceStub(channel, options)
  }
  
  def bindService(serviceImpl: RulesService, executionContext: scala.concurrent.ExecutionContext): _root_.io.grpc.ServerServiceDefinition =
    _root_.io.grpc.ServerServiceDefinition.builder("com.chargerlink.gateway.analyser.RulesService")
    .addMethod(
      METHOD_SET_ALARM_RULE,
      _root_.io.grpc.stub.ServerCalls.asyncUnaryCall(new _root_.io.grpc.stub.ServerCalls.UnaryMethod[com.chargerlink.gateway.analyser.AlarmRule, com.chargerlink.gateway.analyser.Result] {
        override def invoke(request: com.chargerlink.gateway.analyser.AlarmRule, observer: _root_.io.grpc.stub.StreamObserver[com.chargerlink.gateway.analyser.Result]): Unit =
          serviceImpl.setAlarmRule(request).onComplete(com.trueaccord.scalapb.grpc.Grpc.completeObserver(observer))(
            executionContext)
      }))
    .addMethod(
      METHOD_REMOVE_ALARM_RULE,
      _root_.io.grpc.stub.ServerCalls.asyncUnaryCall(new _root_.io.grpc.stub.ServerCalls.UnaryMethod[com.chargerlink.gateway.analyser.AlarmRule, com.chargerlink.gateway.analyser.Result] {
        override def invoke(request: com.chargerlink.gateway.analyser.AlarmRule, observer: _root_.io.grpc.stub.StreamObserver[com.chargerlink.gateway.analyser.Result]): Unit =
          serviceImpl.removeAlarmRule(request).onComplete(com.trueaccord.scalapb.grpc.Grpc.completeObserver(observer))(
            executionContext)
      }))
    .build()
  
  def blockingStub(channel: _root_.io.grpc.Channel): RulesServiceBlockingStub = new RulesServiceBlockingStub(channel)
  
  def stub(channel: _root_.io.grpc.Channel): RulesServiceStub = new RulesServiceStub(channel)
  
  def javaDescriptor: _root_.com.google.protobuf.Descriptors.ServiceDescriptor = com.chargerlink.gateway.analyser.RulesProto.javaDescriptor.getServices().get(0)
  
}