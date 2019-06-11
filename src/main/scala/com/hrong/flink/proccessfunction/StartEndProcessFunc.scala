package com.hrong.flink.proccessfunction

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector


/**
  * 基于输入的开始和结束事件计算时间间隔
  */
class StartEndProcessFunc extends KeyedProcessFunction[String, (String, String), (String, Long)] {
  var startTime: ValueState[Long] = _

  override def processElement(value: (String, String),
                              ctx: KeyedProcessFunction[String, (String, String),
                                (String, Long)]#Context,
                              out: Collector[(String, Long)]): Unit = {
    if (value._1.equalsIgnoreCase("start")) {
      // 如果接收到的是开始事件，则设置startTime
      startTime.update(ctx.timestamp)
      // 从startTime开始注册一个时长为4小时的timer
      ctx.timerService.registerEventTimeTimer(ctx.timestamp + 4 * 60 * 60 * 1000)
    } else if (value._1.equalsIgnoreCase("end")) {
      val sTime = startTime.value
      if (sTime != null) {
        out.collect((value._1, ctx.timestamp - sTime))
        // 清除状态
        startTime.clear()
      }
    }
  }

  override def open(parameters: Configuration): Unit = {
    getRuntimeContext.getState(new ValueStateDescriptor[Long]("startTime", classOf[Long]))
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, (String, String),
                      (String, Long)]#OnTimerContext,
                      out: Collector[(String, Long)]): Unit = {
    startTime.clear()
  }
}
