package com.hrong.flink.window

import java.text.SimpleDateFormat
import java.util.Date

import com.hrong.flink.model.Income
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  * countWindow(3L)表示窗口中元素数量达到阈值3，则开启下一个窗口
  * 输入：
  *     1:1
  *     1:2
  *     1:2
  *     1:1
  *     1:5
  *     1:1
  *     1:2
  *     1:2
  *     1:1
  *     1:1
  *     1:2
  * 输出：
  *     receive : 1:1
  *     receive : 1:2
  *     receive : 1:2
  *     2019-05-16 11:38:42 compute result::6> Income(1,5.0)
  *     receive : 1:1
  *     receive : 1:1
  *     receive : 1:2
  *     2019-05-16 11:38:42 compute result::6> Income(1,4.0)
  * 可以看出来只要窗口中元素数量达到阈值3，则开启下一个窗口
  *
  */
object CountWindow {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val data: DataStream[String] = env.socketTextStream("localhost", 9999)
    val incomeData = data.map(item => {
      println(s"receive : $item")
      try {
        val storeId_money = item.split(":")
        val storeId = storeId_money(0).toInt
        val money = storeId_money(1).toDouble
        Income(storeId, money)
      } catch {
        case _: Exception =>
          println(s"输入数据${item}不满足要求")
          Income(0, 0)
      }
    })
    //翻滚计数窗口
    incomeData
      .keyBy("storeId")
      .timeWindow(Time.seconds(10L))
      .trigger(new CountTriggerWithTimeout[Income](3, TimeCharacteristic.ProcessingTime))
      .apply[Income]((key: Tuple, window: TimeWindow, inputIter: Iterable[Income], out: Collector[Income]) => {
        println(window.getStart, window.getEnd)
        inputIter.foreach(item => out.collect(item))
      }).print(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date)+" compute result:")


    env.execute(this.getClass.getName)
    System.out.println(System.currentTimeMillis());
  }
}
