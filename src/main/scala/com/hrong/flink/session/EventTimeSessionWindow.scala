package com.hrong.flink.session

import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows
import org.apache.flink.streaming.api.windowing.time.Time

object EventTimeSessionWindow {
  def main(args: Array[String]): Unit = {
    //创建运行时环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.getConfig.disableSysoutLogging()
    //创建模拟测试数据
    val text = env.fromElements("flink hadoop", "spark hive").flatMap(_.split("\\s+"))
    text
      .map((_, 1))
      .keyBy(0)
      .window(EventTimeSessionWindows.withGap(Time.seconds(10L)))
      .sum(1)
  }

}
