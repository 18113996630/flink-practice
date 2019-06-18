package com.hrong.flink.checkpoint

import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object CheckPointFunc01 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // checkpoint的间隔时间
    env.enableCheckpointing(1000L)
    // 启用exactly-once语义，使得barrier必须对齐
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    // 两次checkpoint的最短间隔
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500L)
    // 超时时间
    env.getCheckpointConfig.setCheckpointTimeout(5000L)
    // 当checkpoint失败时task是否继续，默认为false
    env.getCheckpointConfig.setFailOnCheckpointingErrors(false)
    // 保留的checkpoint的数目
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    // 在task取消时是否保存checkpoint文件
    env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

  }
}
