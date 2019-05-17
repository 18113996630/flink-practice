package com.hrong.flink.table_sql

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row

object TableQuery {
  def main(args: Array[String]): Unit = {
    //流式数据
    val senv = StreamExecutionEnvironment.getExecutionEnvironment
    val stableEnv = StreamTableEnvironment.create(senv)
    val dataStream = senv.fromElements(
      ("张三", "male", 20), ("李四", "male", 22),
      ("秋秋", "female", 24), ("春春", "female", 18)
    )

    stableEnv.registerTable("s_people", stableEnv.fromDataStream(dataStream, 'name, 'gender, 'age))
    stableEnv.scan("s_people")
      //如果这样写filter不会起作用
      .filter('age == "22")
      .select('name, 'gender, 'age)
      .toAppendStream[Row]
      .print()

//    stableEnv.sqlQuery(
//      """
//        |select name, gender, age
//        | from s_people
//        | where age != 18
//      """.stripMargin).toAppendStream[Row].print()
    senv.execute(this.getClass.getName)
  }
}
