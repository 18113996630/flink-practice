package com.hrong.flink.parameter

import java.util

import com.hrong.flink.model.{Cost, WeightInfo}
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.configuration.Configuration


object ParameterFunction02 {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val weightInfo = env.fromElements(
      WeightInfo("香蕉", 2),
      WeightInfo("枇杷", 10),
      WeightInfo("苹果", 1)
    )
    //new一个Configuration，Configuration是ExecutionConfig.GlobalJobParameters的子类
    val conf = new Configuration()
    conf.setInteger("香蕉", 5)
    conf.setInteger("枇杷", 1)
    conf.setInteger("苹果", 10)
    env.getConfig.setGlobalJobParameters(conf)

    weightInfo.map(new RichMapFunction[WeightInfo, Cost] {
      private var map: util.Map[String, String] = _

      override def open(parameters: Configuration): Unit = {
        super.open(parameters)
        val globalParameters = getRuntimeContext.getExecutionConfig.getGlobalJobParameters
        map = globalParameters.toMap
      }

      override def map(value: WeightInfo): Cost = {
        val price = map.getOrDefault(value.name, "")
        if (!price.isEmpty) {
          return Cost(value.name, price.toInt * value.weight)
        }
        Cost(value.name, 0.0)
      }
    }).print()
  }
}
