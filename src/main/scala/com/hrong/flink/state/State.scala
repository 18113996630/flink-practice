package com.hrong.flink.state

import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}

/**
  *
  */
object State {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val DataStream = env.fromElements(("one", 1), ("two", 2), ("one", 1), ("two", 3), ("three", 2))
    DataStream.keyBy(_._1)
      //先根据输入数据的第一个字段进行分组
      //根据输入，将第二个字段进行累加求和(区分第一次求和与非第一次求和)
      .mapWithState((in: (String, Int), count: Option[Int]) =>
        count match {
          case Some(c) => ((in._1, c + in._2), Some(c + in._2))
          case None => ((in._1, in._2), Some(in._2))
        }
      ).print()
    env.execute(this.getClass.getName)
  }
}
