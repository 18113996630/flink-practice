package com.hrong.flink.cache

import com.hrong.flink.model.{Cost, WeightInfo}
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.configuration.Configuration

import scala.collection.mutable.ListBuffer
import scala.io.Source

/**
  *  env.registerCachedFile(filePath,name)注册
  *  org.apache.flink.api.common.functions.RuntimeContext#getDistributedCache.getFile("name")获取本地缓存数据
  */
object CacheFile {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    //在分布式缓存中将本地的文件进行注册
    env.registerCachedFile("file:///D:/workspace/workspace_hadoop/flink-practice/src/main/resources/cachefile", "fruitsPrice")

    val weight = env.fromElements(
      WeightInfo("香蕉", 2),
      WeightInfo("枇杷", 10),
      WeightInfo("苹果", 1)
    )

    weight.map(new RichMapFunction[WeightInfo, Cost] {
      private val fruitsPrice: ListBuffer[String] = new ListBuffer[String]

      //获取缓存文件，将内容添加进priceInfo
      override def open(parameters: Configuration): Unit = {
        super.open(parameters)
        val file = getRuntimeContext.getDistributedCache.getFile("fruitsPrice")
        val iterator = Source.fromFile(file.getAbsolutePath).getLines()
        iterator.foreach(line => fruitsPrice.append(line))
      }

      override def map(value: WeightInfo): Cost = {
        for (info <- fruitsPrice) {
          val arr = info.split("\\s+")
          val length = arr.length
          println(s"price info: $info , length is : $length")
          if (length == 2) {
            val name = arr(0)
            val price = arr(1).toInt
            if (value.name.endsWith(name)) {
              return Cost(name, value.weight * price)
            }
          }
        }
        //出现无法对应的情况将money置为0.0
        Cost(value.name, 0.0)
      }
    }).print()
  }
}
