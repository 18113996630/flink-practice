package com.hrong.flink.dataset_function

import org.apache.flink.api.scala.{ExecutionEnvironment, _}

object JoinFunction01 {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.getConfig.disableSysoutLogging()

    val stuDataSet = env.fromElements(
      (1, "张三", "男", 21),
      (2, "彭霞", "女", 18),
      (3, "李四", "男", 20),
      (4, "李莉", "女", 23),
      (5, "倩倩", "女", 21)
    )
    val scoreDataSet = env.fromElements(
      (1, 90),
      (2, 84),
      (3, 80),
      (4, 92),
      (5, 87)
    )
    //where是指出左边DataSet的Join列，equalTo是指出右边DataSet的Join列
    val res = stuDataSet.join(scoreDataSet)
      .where(0)
      .equalTo(0)
    res.print()

  }
}
