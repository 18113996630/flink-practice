package com.hrong.flink.wordcount

import java.lang
import java.util.concurrent.TimeUnit

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.windowing.WindowFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object Test {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)
    val ele = env.fromElements(
      Word("a", 20, "w"), Word("b", 20, "w"), Word("c", 21, "m"), Word("d", 23, "w")
    )
    ele.print("source")
    val aggFunction = new AggregateFunction[Word, WordAcc, Int] {
      override def createAccumulator(): WordAcc = new WordAcc

      override def add(value: Word, accumulator: WordAcc): WordAcc = {
        accumulator.count += 1
        accumulator.name = value.name
        accumulator
      }

      override def getResult(accumulator: WordAcc): Int = accumulator.count

      override def merge(a: WordAcc, b: WordAcc): WordAcc = {
        a.count = a.count + b.count
        a
      }
    }
    val windowFunction = new WindowFunction[WordAcc, WordAcc, String, TimeWindow] {
      override def apply(key: String, window: TimeWindow, input: lang.Iterable[WordAcc], out: Collector[WordAcc]): Unit = {

      }
    }
    ele.keyBy("gender")
//      .timeWindow(Time.seconds(1))
      .window(TumblingEventTimeWindows.of(Time.of(1, TimeUnit.SECONDS)))
      .aggregate(aggFunction)
      .print("result")
    env.execute(this.getClass.getSimpleName)
  }
}

case class Word(name: String, age: Int, gender: String)

class WordAcc {
  var name: String = ""
  var count: Int = 0
}
