package com.hrong.flink.dataset_function

import java.lang

import org.apache.flink.api.common.functions.GroupReduceFunction
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.util.Collector

object ReduceGroupFunction01 {
  def main(args: Array[String]): Unit = {
    //创建运行时环境
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.getConfig.disableSysoutLogging()
    //创建模拟测试数据
    val text = env.fromElements("flink hadoop", "spark hive hadoop flink", "flink").flatMap(_.split("\\s+"))
    //一般是先分组，然后根据每组的数据进行计算
    val text2 = text.map((_, 1)).groupBy(0).reduceGroup(new GroupReduceFunction[(String, Int), (String, Int)] {
      override def reduce(values: lang.Iterable[(String, Int)], out: Collector[(String, Int)]): Unit = {
        val iterator = values.iterator()
        var word = ""
        var cnt = 0
        while (iterator.hasNext) {
          val item = iterator.next()
          word = item._1
          cnt += item._2
        }
        out.collect((word, cnt))
      }
    })
    text2.print()
  }
}
