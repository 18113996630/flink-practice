package com.hrong.flink.table_sql

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row

object TableQueryBatch {
  def main(args: Array[String]): Unit = {
    //batch数据
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv = BatchTableEnvironment.create(env)

    val dataSet: DataSet[(String, String, Int)] = env.fromElements(
      ("张三", "male", 20), ("李四", "male", 22),
      ("秋秋", "female", 24), ("春春", "female", 18)
    )
    // 注册时指定字段名
    tableEnv.registerTable("s_people", tableEnv.fromDataSet(dataSet, 'name, 'gender, 'age))
    //类似select from ***
    val resultTab = tableEnv.scan("s_people")
      //如果这样写filter不会起作用 .filter('age == 22)
      .filter('age === 22)
      .select('name, 'gender, 'age)

    //注册时不指定字段名
    tableEnv.registerTable("b_people", tableEnv.fromDataSet(dataSet, '_2, '_3, '_1))

    tableEnv.scan("b_people").printOnTaskManager("查询结果：")
    /**
      * 将结果转换为DataSet
      */
    val set = tableEnv.toDataSet[Row](resultTab)
    set.printOnTaskManager("查询结果(dataset-row):")
    val tuple3Set = tableEnv.toDataSet[(String,String,Int)](resultTab)
    tuple3Set.printOnTaskManager("查询结果(dataset-tuple3):")
    //查询结果(dataset-row):> 李四,male,22
    //查询结果(dataset-tuple3):> (李四,male,22)
    env.execute(this.getClass.getName)
  }
}
