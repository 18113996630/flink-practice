package com.hrong.flink.async

import java.util.concurrent.TimeUnit

import org.apache.flink.streaming.api.scala.async.{AsyncFunction, ResultFuture}
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}

object Async {
  def main(args: Array[String]): Unit = {
    val senv = StreamExecutionEnvironment.getExecutionEnvironment
    val elements = senv.fromElements(1, 2, 4, 3, 1)
    val timeout = 5L
    //异步操作
    AsyncDataStream.orderedWait(elements, new AsyncPrint, timeout, TimeUnit.SECONDS)
    senv.execute(this.getClass.getSimpleName)
  }
}

class AsyncPrint extends AsyncFunction[Int, Int] {
  override def asyncInvoke(input: Int, resultFuture: ResultFuture[Int]): Unit = {
    println("start deal：" + input)
    Thread.sleep(2000)
    resultFuture.complete(Seq(input * 20))
  }
}
