package com.hrong.flink.sink

import com.hrong.flink.model.Stu
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment,_}

object SinkTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val inputData = env.fromElements(
      Stu(9, 2, "张三", 23),
      Stu(10, 1, "李四", 22),
      Stu(11, 3, "王五", 21),
      Stu(12, 4, "赵六", 20)
    )
    inputData.addSink(new MysqlSinkScala)
    env.execute(this.getClass.getName)
  }
}
