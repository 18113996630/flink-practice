package com.hrong.flink.time

import java.text.SimpleDateFormat
import java.util.Date

import com.hrong.flink.model.IncomeWithEventTime
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * 输入：
  *     receive : 1:0:1557994143000
  *     2019-05-16 16:09:17 compute result::6> IncomeWithEventTime(1,0.0,1557994143000)
  *     receive : 1:10:1557994138000
  *     2019-05-16 16:09:17 compute result::6> IncomeWithEventTime(1,10.0,1557994138000)
  * 第二条数据的eventTime是小于第一条数据的eventTime的，但是processingTime是大于第一条的
  * 所以会取第二条数据
  */
object ProcessingTime {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

    val data: DataStream[String] = env.socketTextStream("s102", 9999)
    val incomeData = data.map(item => {
      println(s"receive : $item")
      try {
        val storeId_money = item.split(":")
        val storeId = storeId_money(0).toInt
        val money = storeId_money(1).toDouble
        val time = storeId_money(2).toLong
        IncomeWithEventTime(storeId, money, time)
      } catch {
        case _: Exception =>
          println(s"输入数据${item}不满足要求")
          IncomeWithEventTime(0, 0, 9999)
      }
    })
    val moneySum = incomeData
      .keyBy("storedId")
      .timeWindow(Time.seconds(5L))
      .max("money")
    moneySum.print(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date) + " compute result:")
    env.execute(this.getClass.getName)
  }
}
