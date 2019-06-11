package com.hrong.flink.watermark

import org.apache.commons.lang3.time.FastDateFormat
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ArrayBuffer

/**
  * 延迟测试
  * flink对于迟到数据的处理：
  * 如果业务上对迟到的数据有需求，则可以设置允许迟到的时间，那么每个窗口可以在允许等待范围内继续等待迟到的数据
  * 只要当前的watermark时间 < window结束时间+allowedLateness
  * 那么就算该窗口已经进行过计算，再次接收到属于该窗口的数据，仍会继续参与窗口计算。
  * 详细讲解博客地址：https://blog.csdn.net/hlp4207/article/details/90717905
  */
object WaterMarkFunc02 {
  // 线程安全的时间格式化对象
  val sdf: FastDateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss:SSS")

  def main(args: Array[String]): Unit = {
    val hostName = "s102"
    val port = 9000
    val delimiter = '\n'
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 将EventTime设置为流数据时间类型
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    val streams: DataStream[String] = env.socketTextStream(hostName, port, delimiter)
    import org.apache.flink.api.scala._
    val data = streams.map(data => {
      // 输入数据格式：name:时间戳
      // flink:1559223685000
      try {
        val items = data.split(":")
        (items(0), items(1).toLong)
      } catch {
        case _: Exception => println("输入数据不符合格式：" + data)
          ("0", 0L)
      }
    }).filter(data => !data._1.equals("0") && data._2 != 0L)

    //为数据流中的元素分配时间戳，并定期创建水印以监控事件时间进度
    val waterStream: DataStream[(String, Long)] = data.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[(String, Long)] {
      // 事件时间
      var currentMaxTimestamp = 0L
      val maxOutOfOrderness = 3000L
      var lastEmittedWatermark: Long = Long.MinValue

      // Returns the current watermark
      override def getCurrentWatermark: Watermark = {
        // 允许延迟三秒
        val potentialWM = currentMaxTimestamp - maxOutOfOrderness
        // 保证水印能依次递增
        if (potentialWM >= lastEmittedWatermark) {
          lastEmittedWatermark = potentialWM
        }
        new Watermark(lastEmittedWatermark)
      }

      // Assigns a timestamp to an element, in milliseconds since the Epoch
      override def extractTimestamp(element: (String, Long), previousElementTimestamp: Long): Long = {
        // 将元素的时间字段值作为该数据的timestamp
        val time = element._2
        if (time > currentMaxTimestamp) {
          currentMaxTimestamp = time
        }
        val outData = String.format("key: %s    EventTime: %s    waterMark:  %s",
          element._1,
          sdf.format(time),
          sdf.format(getCurrentWatermark.getTimestamp))
        println(outData)
        time
      }
    })
    val lateData = new OutputTag[(String,Long)]("late")
    val result: DataStream[String] = waterStream.keyBy(0)// 根据name值进行分组
      .window(TumblingEventTimeWindows.of(Time.seconds(5L)))// 5s跨度的基于事件时间的翻滚窗口
      .allowedLateness(Time.seconds(2L))
      .sideOutputLateData(lateData)
      .apply(new WindowFunction[(String, Long), String, Tuple, TimeWindow] {
        override def apply(key: Tuple, window: TimeWindow, input: Iterable[(String, Long)], out: Collector[String]): Unit = {
          val timeArr = ArrayBuffer[String]()
          val iterator = input.iterator
          while (iterator.hasNext) {
            val tup2 = iterator.next()
            timeArr.append(sdf.format(tup2._2))
          }
          val outData = String.format("key: %s    data: %s    startTime:  %s    endTime:  %s",
            key.toString,
            timeArr.mkString("-"),
            sdf.format(window.getStart),
            sdf.format(window.getEnd))
          out.collect(outData)
        }
      })
    result.print("window计算结果:")

    val late = result.getSideOutput(lateData)
    late.print("迟到的数据:")

    env.execute(this.getClass.getName)
  }
}
