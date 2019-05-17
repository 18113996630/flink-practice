package com.hrong.flink.time

import java.text.SimpleDateFormat
import java.util.Date

import com.hrong.flink.model.IncomeWithEventTime
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * 输入及输出：
  *          receive : 1:10:1557991597000
  *          receive : 1:0:1557991605000
  *          receive : 1:2:1557991600000
  *          receive : 1:4:1557991598000
  *          receive : 1:9:1557991602000
  *          receive : 1:9:1557991604000
  *          receive : 1:4:1557991599000
  *          receive : 1:6:1557991606000
  *          receive : 1:0:1557991603000
  *          receive : 1:4:1557991601000
  *          receive : 1:8:1557991607000
  *          2019-05-16 15:26:49 compute result::6> IncomeWithEventTime(1,10.0,1557991597000)
  *          receive : 1:7:1557991632000
  *          receive : 1:7:1557991634000
  *          receive : 1:0:1557991640000
  *          receive : 1:7:1557991635000
  *          receive : 1:6:1557991637000
  *          receive : 1:6:1557991639000
  *          receive : 1:7:1557991638000
  *          receive : 1:2:1557991633000
  *          receive : 1:0:1557991636000
  *          receive : 1:6:1557991641000
  *          2019-05-16 15:26:49 compute result::6> IncomeWithEventTime(1,9.0,1557991600000)
  *          2019-05-16 15:26:49 compute result::6> IncomeWithEventTime(1,8.0,1557991605000)
  *          receive : 1:1:1557991642000
  *          2019-05-16 15:26:49 compute result::6> IncomeWithEventTime(1,7.0,1557991632000)
  * 【注意：传入的时间戳必须精确到毫秒】
  */
object EventTime {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

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
      //对于时间戳是单调递增的情况调用该方法
      .assignAscendingTimestamps(_.time)
      .keyBy("storedId")
      .timeWindow(Time.seconds(5L))
      .max("money")
    moneySum.print(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date) + " compute result:")
    env.execute(this.getClass.getName)
  }
}
