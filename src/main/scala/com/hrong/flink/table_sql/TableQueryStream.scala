package com.hrong.flink.table_sql

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.scala.{StreamTableEnvironment, _}
import org.apache.flink.types.Row

object TableQueryStream {
  def main(args: Array[String]): Unit = {
    //流式数据
    val senv = StreamExecutionEnvironment.getExecutionEnvironment
    val stableEnv: StreamTableEnvironment = StreamTableEnvironment.create(senv)

    val dataStream: DataStream[(String, String, Int)] = senv.fromElements(
      ("张三", "male", 20), ("李四", "male", 22),
      ("秋秋", "female", 24), ("春春", "female", 18)
    )

    stableEnv.registerTable("s_people", stableEnv.fromDataStream(dataStream, 'name, 'gender, 'age))
    //类似select from ***
    val resultTab = stableEnv.scan("s_people")
      //如果这样写filter不会起作用 .filter('age == 22)
      .filter('age === 22)
      .select('name, 'gender, 'age)

    /**
      * 将结果转换为DataStream
      */
    val stream: DataStream[Row] = stableEnv.toAppendStream[Row](resultTab)
    val tuple3Stream = stableEnv.toAppendStream[(String, String, Int)](resultTab)
    stream.print("查询结果(DataStream-row)：")
    tuple3Stream.print("查询结果(DataStream-tuple3)：")
    /** **********************以下为附带数据状态信息的方法 ******************************/
    val retractStream: DataStream[(Boolean, Row)] = stableEnv.toRetractStream[Row](resultTab)
    // boolean结果为该数据的类型：true：插入  false：撤回
    retractStream.print("查询结果(DataStream-row)：")
    //查询结果：:1> 李四,male,22
    //查询结果：:5> (true,李四,male,22)

    //    stableEnv.sqlQuery(
    //      """
    //        |select name, gender, age
    //        | from s_people
    //        | where age = 18
    //      """.stripMargin).toAppendStream[Row].print()
    senv.execute(this.getClass.getName)
  }
}
