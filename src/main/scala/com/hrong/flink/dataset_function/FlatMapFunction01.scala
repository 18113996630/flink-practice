package com.hrong.flink.dataset_function

import org.apache.flink.api.scala.{ExecutionEnvironment, _}

object FlatMapFunction01 {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.getConfig.disableSysoutLogging()
    //创建模拟测试数据
    val text = env.fromElements("flink hadoop", "spark hive")
    //使用flatMap来进行数据的切割,将每次的数据都作用于该function
    val text2 = text.flatMap(_.split("\\s+"))
    text2.print()
  }

}
