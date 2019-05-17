package com.hrong.flink.dataset_function

import java.lang

import org.apache.flink.api.common.functions.MapPartitionFunction
import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.util.Collector

object MapPartitionFunction01 {
  def main(args: Array[String]): Unit = {
    //创建运行时环境
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.getConfig.disableSysoutLogging()
    //创建模拟测试数据
    val text = env.fromElements("flink hadoop", "spark hive").flatMap(_.split("\\s+"))
    //以partition为粒度进行count计算
    //将mapPartition中的方法安装partition作用于DataSet，产生另一个DataSet
    //比较适合没有分组的数据，如果是需要转换单个的元素，更适合用map方法
    //MapPartitionFunction[String, Long] String为输入元素的类型，Long为返回元素的类型，因为是计数，所以是Long
    val text2 = text.mapPartition(new MapPartitionFunction[String, Long]() {
      override def mapPartition(iterable: lang.Iterable[String], collector: Collector[Long]): Unit = {
        var count = 0
        val iterator = iterable.iterator()
        while (iterator.hasNext) {
          iterator.next()
          count += 1
        }
        collector.collect(count)
      }
    })
    text2.print()

    //全体数据加上一个前缀
    val text3 = text.mapPartition(new MapPartitionFunction[String, String] {
      override def mapPartition(values: lang.Iterable[String], out: Collector[String]): Unit = {
        val iterator = values.iterator()
        while (iterator.hasNext) {
          var str = iterator.next()
          str = "prefix-" + str
          out.collect(str)
        }
      }
    })
    text3.print()
  }
}
