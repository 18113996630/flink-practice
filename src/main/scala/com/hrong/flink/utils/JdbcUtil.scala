package com.hrong.flink.utils

import java.sql.{Connection, DriverManager}

object JdbcUtil {
  def getConnection: Connection ={
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://localhost:3306/test"
    val username = "root"
    val password = "123456"
    //1.加载驱动
    Class.forName(driver)
    //2.创建连接
    val connection = DriverManager.getConnection(url, username, password)
    connection
  }
}
