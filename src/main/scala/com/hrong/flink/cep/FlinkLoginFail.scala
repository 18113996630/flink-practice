package com.hrong.flink.cep

import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.api.scala._
import org.apache.flink.cep.scala.CEP

object FlinkLoginFail {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val loginEventStream = env.fromCollection(List(
      LoginEvent("1", "192.168.0.1", "fail"),
      LoginEvent("1", "192.168.0.2", "fail"),
      LoginEvent("1", "192.168.0.3", "success"),
      LoginEvent("1", "192.168.0.5", "fail"),
      LoginEvent("2", "192.168.10.10", "success") ))

    val loginFailPattern = Pattern.begin[LoginEvent]("begin")
      .where(_.ltype.equals("fail")) .followedBy("next")
      .where(_.ltype.equals("fail"))
      .within(Time.seconds(1))

    val patternStream = CEP.pattern(loginEventStream, loginFailPattern)

    val loginFailDataStream = patternStream
      .select((pattern) => {
        val first = pattern.getOrElse("begin", null).iterator.next()
        val second = pattern.getOrElse("next", null).iterator.next()
        LoginEvent(second.userid, second.ip, second.ltype)
      })

    loginFailDataStream.print()

    env.execute("FlinkLoginFail")
  }
}

case class LoginEvent(userid:String,ip:String,ltype:String)