package com.hrong.flink.tolerance

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.restartstrategy.RestartStrategies.{FixedDelayRestartStrategyConfiguration, RestartStrategyConfiguration}
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.ExecutionEnvironment

/**
  * flink支持容错设置,当操作失败了，可以在指定重试的启动时间和重试的次数.有两种设置方式
  * 1.通过配置文件，进行全局的默认设定
  * 通过配置flink-conf.yaml来设定全局容错
  * 设定出错重试3次: execution-retries.default: 3
  * 设定重试间隔时间5秒: execution-retries.delay: 5 s
  * 2.通过程序的api进行设定
  **/
object FaultTolerance01 {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    //固定延迟重启策略配置
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.seconds(10L)))
    env.fromElements(Array(1, 2, 3, 4)).map(item => {
      item.foreach(_ + 1)
      item
    }).print()
  }
}
