package com.chargerlink.hta.report

import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.Encoders

// "operator": "string, required, 归属运营商",
// "brand": "string, required, 车辆品牌",
// "vin": "string, required, 车辆识别代号",
// "plate": "string, required, 车牌号",
case class Vehicle(accessId: String, operator: String, brand: String, vin: String, plate: String)

/**
  * deviceId 终端设备ID。格式由对应类型设备自行定义, 设备类型和设备ID组成设备的全局唯一标识。
  * dataTime 数据采集的时间(北京时间的毫秒时间戳)。单位：毫秒。
  * currentMileage 行驶里程
  */
case class DataVehOverall(deviceId: String, dataTime: Long, currentMileage: Long)

case class Average(var sum: Long, var count: Long)

/**
  * Created by Cuiwx on 2017/6/19/019.
  */
object VehicleAverageAsUserDefined extends Aggregator[DataVehOverall, Average, Double] {
  // A zero value for this aggregation. Should satisfy the property that any b + zero = b
  def zero: Average = Average(0L, 0L)
  // Combine two values to produce a new value. For performance, the function may modify `buffer`
  // and return it instead of constructing a new object
  def reduce(buffer: Average, dataVehOverall: DataVehOverall): Average = {
    buffer.sum += dataVehOverall.currentMileage
    buffer.count += 1
    buffer
  }
  // Merge two intermediate values
  def merge(b1: Average, b2: Average): Average = {
    b1.sum += b2.sum
    b1.count += b2.count
    b1
  }
  // Transform the output of the reduction
  def finish(reduction: Average): Double = reduction.sum.toDouble / reduction.count
  // Specifies the Encoder for the intermediate value type
  def bufferEncoder: Encoder[Average] = Encoders.product
  // Specifies the Encoder for the final output value type
  def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}
