package com.hrong.flink.cache

import java.util

import com.hrong.flink.model.{Cost, FruitsPrice, WeightInfo}
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.configuration.Configuration

/**
  * withBroadcastSet(DataSet, name) 将某个数据集作为广播集添加到该操作
  * org.apache.flink.api.common.functions.AbstractRichFunction#getRuntimeContext().getBroadCastVariable(name)
  * 根据name值获取广播数据
  */
object BroadCastVariables {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val fruits = env.fromElements(
      FruitsPrice("苹果", 10),
      FruitsPrice("香蕉", 5),
      FruitsPrice("枇杷", 1)
    )
    val weight = env.fromElements(
      WeightInfo("香蕉", 2),
      WeightInfo("枇杷", 10),
      WeightInfo("苹果", 1)
    )
    weight.map(new RichMapFunction[WeightInfo, Cost] {
      private var fruitsPrice: util.List[FruitsPrice] = _

      override def open(parameters: Configuration): Unit = {
        super.open(parameters)
//        org.apache.flink.api.common.functions.AbstractRichFunction#getRuntimeContext().getBroadCastVariable(String)`
        fruitsPrice = getRuntimeContext.getBroadcastVariable("fruitsPrice")
      }

      override def map(value: WeightInfo): Cost = {
        var cnt = 0
        while (cnt < fruitsPrice.size()) {
          val item = fruitsPrice.get(cnt)
          println(s"priceInfo: $item , now compute the fruit is : $value")
          val name = item.name
          val unitPrice = item.unitPrice
          if (value.name.equals(name)) {
            val cost = unitPrice * value.weight
            return Cost(name, cost)
          }
          cnt += 1
        }
        //出现无法对应的情况将money置为0.0
        Cost(value.name, 0.0)
      }
    }).withBroadcastSet(fruits, "fruitsPrice").print()
  }
}
