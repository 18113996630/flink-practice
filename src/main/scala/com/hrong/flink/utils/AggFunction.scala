package com.hrong.flink.utils

import com.hrong.flink.model.KafkaData
import org.apache.flink.api.common.functions.AggregateFunction

/**
  * 自定义的累加器
  */
class AggFunction extends AggregateFunction[KafkaData, (KafkaData, Long), (KafkaData, Long)] {
  //创建一个accumulator
  override def createAccumulator(): (KafkaData, Long) = (null, 1)
  //相加操作
  override def add(value: KafkaData, accumulator: (KafkaData, Long)): (KafkaData, Long) = {
    var data = accumulator._1
    var count = accumulator._2
    if (data == null)
      data = value
    count += 1
    (data, count)
  }
  //获取值操作
  override def getResult(accumulator: (KafkaData, Long)): (KafkaData, Long) = accumulator
  //合并数据
  override def merge(a: (KafkaData, Long), b: (KafkaData, Long)): (KafkaData, Long) = (a._1, a._2 + b._2)
}
