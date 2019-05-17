package com.hrong.flink.source

import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment,_}

object SourceTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val sourceData = env.addSource(new MysqlSourceScala)
    sourceData.print()
    env.execute("source Job starting")
  }
}
