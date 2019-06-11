package com.hrong.flink.cep

import java.util

import akka.actor.FSM
import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, AssignerWithPunctuatedWatermarks}
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.watermark.Watermark

/**
  * flink-cep示例代码
  */
object CepFun01 {
  def main(args: Array[String]): Unit = {
    val senv = StreamExecutionEnvironment.getExecutionEnvironment
    senv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val input = senv.fromElements(
      (Event(1, "start", 1.0), 5L),
      (Event(2, "middle", 2.0), 1L),
      (Event(3, "end", 3.0), 3L),
      (Event(4, "end", 4.0), 10L),
      (Event(5, "middle", 5.0), 7L),
      (Event(5, "middle", 5.0), 100L)
    ).assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks[(Event, Long)] {
      // 事件时间
      var currentMaxTimestamp = 0L
      val maxOutOfOrderness = 2000L
      var lastEmittedWatermark: Long = Long.MinValue


      override def extractTimestamp(element: (Event, Long), previousElementTimestamp: Long): Long = {
        val timestamp = element._2
        if (timestamp > currentMaxTimestamp) {
          currentMaxTimestamp = timestamp
        }
        timestamp
      }

      override def checkAndGetNextWatermark(lastElement: (Event, Long), extractedTimestamp: Long): Watermark = {
        val potentialWM = currentMaxTimestamp - maxOutOfOrderness
        // 保证水印能依次递增
        if (potentialWM >= lastEmittedWatermark) {
          lastEmittedWatermark = potentialWM
        }
        new Watermark(lastEmittedWatermark)
      }
    }).map(_._1)
    val pattern = Pattern.begin[Event]("start").where(event => event.id == 3)
      .next("next")
      .where(event => event.id == 5)
    val cepResult: PatternStream[Event] = CEP.pattern(input, pattern)
    cepResult.select(new PatternSelectFunction[Event, Event] {
      override def select(map: util.Map[String, util.List[Event]]): Event = {
        val set = map.entrySet()
        for(event <- set){

        }
      }
    })
  }
}

case class Event(id: Int, name: String, score: Double)
