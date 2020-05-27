package com.hrong.flink.sideoutput

import com.hrong.flink.utils.WordCountData
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

object SideOutputFunc02 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)
    val words: DataStream[String] = env.fromElements(WordCountData.words)
    val longTag = new OutputTag[String]("long_word")
    val shortTag = new OutputTag[String]("short_word")
    val token = words.process(new ProcessFunction[String, String] {
      override def processElement(value: String, ctx: ProcessFunction[String, String]#Context, out: Collector[String]): Unit = {
        val words = value.toLowerCase().split("\\W+")
        for(word <- words) {
          if (word.length > 5) {
            ctx.output(longTag, word)
          } else {
            ctx.output(shortTag, word)
          }
        }
      }
    })
    token.getSideOutput(longTag).print("long:")
    token.getSideOutput(shortTag).print("short:")
    env.execute(this.getClass.getName)
  }
}