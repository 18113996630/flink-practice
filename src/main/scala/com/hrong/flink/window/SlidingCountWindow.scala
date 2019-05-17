package com.hrong.flink.window

import java.text.SimpleDateFormat
import java.util.Date

import com.hrong.flink.model.Income
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}

/**
  * countWindow(3L, 2L)表示窗口中元素数量达到阈值2，
  * 则开启下一个窗口，但是窗口统计的元素数量是3
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
  *     2019-05-16 11:38:38 compute result::6> Income(1,3.0)
  *     receive : 1:2
  *     receive : 1:1
  *     2019-05-16 11:38:38 compute result::6> Income(1,5.0)
  *     receive : 1:1
  *     receive : 1:2
  *     2019-05-16 11:38:38 compute result::6> Income(1,4.0)
  * 可以看出来只要窗口中元素数量达到阈值2，则开启下一个窗口，第一次统计因为刚开始的第一个窗口只有两个元素，
  * 所以和为3；第二个窗口则是统计的第一个窗口的最后一个值与第二个窗口所有元素的和值，故为5，第三个窗口的统
  * 计值同理
  */
object SlidingCountWindow {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val data: DataStream[String] = env.socketTextStream("s102", 9999)
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
    // 2:滑动间隔的元素数量
    val moneySum = incomeData.keyBy("storeId")
      .countWindow(3L, 2L)
      .sum("money")
    moneySum.print(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date)+" compute result:")
    env.execute(this.getClass.getName)
  }
}
