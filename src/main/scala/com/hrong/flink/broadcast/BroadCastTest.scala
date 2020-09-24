package com.hrong.flink.broadcast

import com.hrong.flink.model.StudentScala
import com.hrong.flink.source.MysqlSourceScala
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * 测试广播流
 * resources/students.sql
 * 输入：id,value
 * 输出：数据库中存在的student对应的 id,name,age,value
 * processBroadcastElement - 处理广播流中的数据
 * processElement - 处理主流的数据
 */
object BroadCastTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val baseInfo = env.socketTextStream("localhost", 9999)
    val studentDesc = new MapStateDescriptor[Integer, StudentScala]("student", BasicTypeInfo.INT_TYPE_INFO, TypeInformation.of(classOf[StudentScala]))
    val studentStream = env.addSource(new MysqlSourceScala).setParallelism(1).broadcast(studentDesc)
    baseInfo.connect(studentStream)

      .process(new BroadcastProcessFunction[String, StudentScala, String] {
        override def processElement(idAndValue: String, readOnlyContext: BroadcastProcessFunction[String, StudentScala, String]#ReadOnlyContext, collector: Collector[String]): Unit = {
          val id = idAndValue.split(",")(0)
          val value = idAndValue.split(",")(1)
          val stu = readOnlyContext.getBroadcastState(studentDesc).get(id.toInt)
          if (stu != null) {
            println("get student from broadcast:" + stu)
            val name = stu.name
            val age = stu.age
            collector.collect(s"$id, $name, $age, $value")
          }
        }

        override def processBroadcastElement(in2: StudentScala, context: BroadcastProcessFunction[String, StudentScala, String]#Context, collector: Collector[String]): Unit = {
          context.getBroadcastState(studentDesc).put(in2.id, in2)
          println("update broadcast:" + in2)
        }
      })
      .addSink(item => println(item))

    env.execute(this.getClass.getSimpleName)
  }
}
