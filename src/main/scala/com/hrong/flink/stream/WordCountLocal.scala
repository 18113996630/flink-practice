package com.hrong.flink.stream

import org.apache.flink.api.common.functions.RichReduceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object WordCountLocal {
  def main(args: Array[String]): Unit = {
    val senv = StreamExecutionEnvironment.getExecutionEnvironment
    val streamData = senv.fromElements("Creates a DataStream from the given non-empty " +
      ". The elements need to be serializable  " +
      "because the framework may move the elements into the cluster if needed.")
    streamData.flatMap(_.toLowerCase().split("\\s+"))
      .map((_, 1))
      .keyBy(0)
      .reduce(new RichReduceFunction[(String, Int)] {
        override def reduce(value1: (String, Int), value2: (String, Int)): (String, Int) = {
          (value1._1, value1._2 + value2._2)
        }
      }).print()
    senv.execute("stream job")
  }
}
