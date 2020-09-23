package com.hrong.flink.state

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
  * 输入：id,value 输出：id,上一次的value,当前的value
  * 判断同一个id，输出两次value超过10的数据
  */
object StateOperator {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val info = env.socketTextStream("localhost", 9999)
    info.map(str => {
      (str.split(",")(0).toInt, str.split(",")(1).toInt)
    })
      .keyBy(_._1)
      // 1、 自定义flastMap，自己维护初始状态，不过官方不推荐设置初始值
//      .flatMap(new StateFlatMapFunc)

      // 2、 使用带状态的算子， 通过判断第一次状态的有无来处理
      .flatMapWithState[(Int, Int, Int), Int]({
      case (item: (Int, Int), None) => (List.empty, Some(item._2))
      case (item: (Int, Int), last: Some[Int]) =>
        val lastValue = last.get
        val diff = (item._2-lastValue).abs
        if (diff > 10) {
          (List((item._1, lastValue, item._2)), Some(item._2))
        } else {
          (List.empty, Some(item._2))
        }
    })



      .addSink(item => println(item))
    env.execute(this.getClass.getSimpleName)
  }
}
class StateFlatMapFunc extends RichFlatMapFunction[(Int, Int), (Int, Int, Int)] {
  /**
    * 使用的时候才会初始化
    */
  lazy val lastValue: ValueState[Int] = getRuntimeContext.getState[Int](new ValueStateDescriptor[Int]("lastValue", classOf[Int], -999))

  override def flatMap(value: (Int, Int), out: Collector[(Int, Int, Int)]): Unit = {
    val last = lastValue.value()
    val diff = (last - value._2).abs
    if (last != -999 && diff > 10) {
      out.collect((value._1, last, value._2))
    }
    lastValue.update(value._2)
  }
}
