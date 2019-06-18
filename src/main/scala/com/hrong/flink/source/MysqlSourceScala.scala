package com.hrong.flink.source

import java.sql.{Connection, PreparedStatement}

import com.hrong.flink.model.StudentScala
import com.hrong.flink.utils.JdbcUtil
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}

class MysqlSourceScala extends RichSourceFunction[StudentScala] {
  private var connection: Connection = _
  private var ps: PreparedStatement = _

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    connection = JdbcUtil.getConnection
    ps = connection.prepareStatement("select id, class_id, name, age from stu")
  }

  override def run(ctx: SourceFunction.SourceContext[StudentScala]): Unit = {
    val resultSet = ps.executeQuery()
    while (resultSet.next()) {
      val id = resultSet.getInt("id")
      val classId = resultSet.getInt("class_id")
      val name = resultSet.getString("name")
      val age = resultSet.getInt("age")
      val stu = StudentScala(id,classId,name,age)
      ctx.collect(stu)
    }
  }

  override def cancel(): Unit = {}

  override def close(): Unit = {
    super.close()
    if (ps != null) {
      ps.close()
    }
    if (connection != null) {
      connection.close()
    }
  }
}
