package com.hrong.flink.table_sql

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.table.api.scala.{BatchTableEnvironment, StreamTableEnvironment, _}
import org.apache.flink.table.catalog.InMemoryExternalCatalog
import org.apache.flink.table.sinks.CsvTableSink
import org.apache.flink.table.sources.CsvTableSource

object TableRegist_1 {
  val csvPath = "E:/workspace/flink-practice/src/main/resources/"

  def main(args: Array[String]): Unit = {
    //流式数据
    val senv = StreamExecutionEnvironment.getExecutionEnvironment
    val stableEnv = StreamTableEnvironment.create(senv)
    val dataStream = senv.fromElements(
      ("张三", 20), ("李四", 22),
      ("王五", 24), ("赵六", 18)
    )
    //注册表的方式：
    //1、通过DataStream
    stableEnv.registerTable("s_people", stableEnv.fromDataStream(dataStream, "name", "age"))
    //2、通过查询结果
    val resTable = stableEnv.scan("s_people").select("_1,_2")
    stableEnv.registerTable("resTable", resTable)

    //注册tableSource
    //TableSource的已有实现类：CsvTableSource、Kafka09TableSource
    stableEnv.registerTableSource("s_source",
      CsvTableSource.builder()
        .path(csvPath + "tablesource.csv")
        .field("name", Types.STRING)
        .field("age", Types.INT)
        .build())

    //注册TableSink
    //TableSink的已有实现类：CsvTableSink、Kafka09TableSink
    stableEnv.registerTableSink("s_sink",
      new CsvTableSink(csvPath + "tablesink.csv", ",", 2, WriteMode.OVERWRITE)
        .configure(Array("name", "age")
          , Array(Types.STRING, Types.INT)))

    //注册外部目录
    //外部目录可以提供有关外部数据库和表的信息，例如其名称，架构，统计信息以及有关如何访问存储在外部数据库，表或文件中的数据的信息
    //已有实现类为：InMemoryExternalCatalog
    val catalogCatalog = new InMemoryExternalCatalog("catalog")
    stableEnv.registerExternalCatalog("s_catalog", catalogCatalog)

    /** *********************************************************************************/
    //离线批数据类似流式数据
    val env = ExecutionEnvironment.getExecutionEnvironment
    val btableEnv = BatchTableEnvironment.create(env)
    val dataSet = env.fromElements(
      ("张三", 20), ("李四", 22),
      ("王五", 24), ("赵六", 18)
    )
    btableEnv.registerTable("b_people", btableEnv.fromDataSet(dataSet))
  }
}
