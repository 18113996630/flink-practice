package com.hrong.flink.wordcount

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment


object WordCountStream {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val streamData = env.socketTextStream("s102",9999)
    streamData.flatMap(_.split("\\W+"))
      .map((_,1))
      .keyBy(0)
      .sum(1)
      .print()
    env.execute(this.getClass.getName)
  }
}
