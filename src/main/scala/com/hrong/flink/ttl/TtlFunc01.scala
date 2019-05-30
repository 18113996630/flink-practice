package com.hrong.flink.ttl

import org.apache.flink.api.common.state.StateTtlConfig
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.common.time.Time

object TtlFunc01 {
  def main(args: Array[String]): Unit = {
    val ttlConfig = StateTtlConfig
      // 生存时间
      .newBuilder(Time.seconds(1))
      .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
      .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
      .build

    val stateDescriptor = new ValueStateDescriptor[String]("text state", classOf[String])
    stateDescriptor.enableTimeToLive(ttlConfig)
  }
}
