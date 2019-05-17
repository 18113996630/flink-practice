package com.hrong.flink.dataset_function

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.ExecutionEnvironment

object ReduceFunction01 {
  def main(args: Array[String]): Unit = {
    //创建运行时环境
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.getConfig.disableSysoutLogging()
    //创建模拟测试数据
    val text = env.fromElements("flink hadoop", "spark hive hadoop flink", "flink").flatMap(_.split("\\s+"))
    //reduce()将输入的数据通过自定义的处理逻辑，返回一个结果
    val text2 = text.reduce((str1, str2) => str1.concat(str2))
    text2.print()
    println("------------------------------------------------")
    val text3 = text.reduce(new ReduceFunction[String]{
      override def reduce(value1: String, value2: String): String = {
        println("The first value to combine:" + value1)
        println("The second value to combine:" + value2)
        value1.concat(value2)
      }
    })
    text3.print()
  }
}
