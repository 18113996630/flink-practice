package com.hrong.flink.dataset_function

import com.hrong.flink.model.LineCount
import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.configuration.Configuration

object MapFunction01 {
  def main(args: Array[String]): Unit = {
    //创建运行时环境
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.getConfig.disableSysoutLogging()
    //创建模拟测试数据
    val text = env.fromElements("flink hadoop", "spark hive").flatMap(_.split("\\s+"))
    //转换为大写，并计算其长度
    val res1 = text.map(str => (str.toUpperCase(), str.trim.length))
    res1.print()
    //使用case class
    val res2 = text.map(line => LineCount(line.toUpperCase(), line.length))
    res2.print()
  }
}

