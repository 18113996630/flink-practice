package com.hrong.flink.proccessfunction

import com.hrong.flink.utils.DateUtil
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
  * KeyedProcessFunction的使用：获取上下文；注册timer；删除timer
  */
object KeyedProcessFunc {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    val info = env.socketTextStream("localhost", 9999)
    info
      .map(str => (str.split(",")(0), str.split(",")(1).toInt))
      .keyBy(_._1)
      .process(new ComputeDiffProcessFunc)
      .addSink(item => println(item))


    env.execute(this.getClass.getSimpleName)
  }
}

/**
  * 十秒内计算差异，否则差异为-999
  */
class ComputeDiffProcessFunc extends KeyedProcessFunction[String, (String, Int), (String, Int)] {
  lazy val abValue: MapState[String, Int] = getRuntimeContext
                                            .getMapState[String, Int](new MapStateDescriptor[String, Int]("abValue",
                                            classOf[String], classOf[Int]))

  override def processElement(value: (String, Int),
                              ctx: KeyedProcessFunction[String, (String, Int), (String, Int)]#Context,
                              out: Collector[(String, Int)]): Unit = {
    if (ctx.getCurrentKey.equals("a")) {
      val currentPro = ctx.timerService().currentProcessingTime()
      abValue.put(value._1, value._2)
      ctx.timerService().registerProcessingTimeTimer(currentPro + 10 * 1000)
      println("注册timeer，当前时间：" + DateUtil.now() + " ，触发时间：" + DateUtil.getTime(currentPro + 10*1000))
    } else if (ctx.getCurrentKey.equals("b")) {
      val aValue = abValue.get("a")
      val diff = (aValue - value._2).abs
      out.collect(("result", diff))
    }
  }

  override def onTimer(timestamp: Long,
                       ctx: KeyedProcessFunction[String, (String, Int), (String, Int)]#OnTimerContext,
                       out: Collector[(String, Int)]): Unit = {
    println("timer触发，当前时间：" + DateUtil.now())
    abValue.remove(ctx.getCurrentKey)
  }
}
