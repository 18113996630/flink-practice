package com.hrong.flink.parameter

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.ExecutionEnvironment

object ParameterFunction01 {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.getConfig.disableSysoutLogging()
    //创建模拟测试数据
    val text = env.fromElements("flink hadoop", "spark hive")
    //使用flatMap来进行数据的切割,将每次的数据都作用于该function
    val text2 = text.flatMap(_.split("\\s+"))
    text2.map(new MapFunc("1")).print()
    //使用构造器来传参
    class MapFunc(prefix: String) extends MapFunction[String, String] {
      override def map(value: String): String = {
        prefix + ":" + value
      }
    }
  }
}
