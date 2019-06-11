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

object SideOutputFunc01 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)
    val words: DataStream[String] = env.fromElements(WordCountData.words)
    val token = words.process(new tokenizer)
    val outputTag = new OutputTag[String]("word")

    token.getSideOutput(outputTag).map(str => str+":"+str.length).print("未统计的数据：")

    token.keyBy(0).window(TumblingEventTimeWindows.of(Time.seconds(3L))).sum(1).print("统计结果")
    env.execute(this.getClass.getName)
  }
}


class tokenizer extends ProcessFunction[String, (String, Long)] {
  private val outputTag = new OutputTag[String]("word")

  override def processElement(input: String, ctx: ProcessFunction[String, (String, Long)]#Context,
                              out: Collector[(String, Long)]): Unit = {
    val words = input.toLowerCase().split("\\W+")
    for (word <- words) {
      if (word.length > 5) {
        out.collect((word, 1L))
      } else {
        ctx.output(outputTag, word)
      }
    }
  }
}
