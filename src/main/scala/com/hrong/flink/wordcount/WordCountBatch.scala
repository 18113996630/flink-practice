package com.hrong.flink.wordcount

import com.hrong.flink.model.WordCount
import org.apache.flink.api.scala.{ExecutionEnvironment, _}

object WordCountBatch {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val data = env.readTextFile("src/main/resources/data.txt")
    val res = data.flatMap(_.split("\\s+"))
      .map(word => WordCount(word, 1))
      .groupBy(0)
      .reduce((wordCount_1, wordCount_2) => WordCount(wordCount_1.word, wordCount_1.count + wordCount_2.count))
    res.print()
  }
}

