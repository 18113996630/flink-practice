package com.hrong.flink.sink

import java.sql.{Connection, PreparedStatement}

import com.hrong.flink.model.StudentScala
import com.hrong.flink.utils.JdbcUtil
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}

class MysqlSinkScala extends RichSinkFunction[StudentScala] {
  private var connection: Connection = _
  private var ps: PreparedStatement = _

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    connection = JdbcUtil.getConnection
    var sql = "insert into stu(id,class_id,name,age) values(?,?,?,?)"
    ps = connection.prepareStatement(sql)
  }

  override def close(): Unit = {
    super.close()
    if (ps != null) {
      ps.close()
    }
    if (connection != null) {
      connection.close()
    }
  }

  override def invoke(value: StudentScala, context: SinkFunction.Context[_]): Unit = {
    try {
      ps.setInt(1, value.id)
      ps.setInt(2, value.class_id)
      ps.setString(3, value.name)
      ps.setInt(4, value.age)
      ps.executeUpdate()
    } catch {
      case e: Exception =>
        val msg = e.getMessage
        println(s"保存出错啦，待保存的数据：$value 出错信息：$msg")
    }

  }
}
