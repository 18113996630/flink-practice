package com.hrong.flink.wordcount

import java.util.Properties

import com.alibaba.fastjson.JSONObject
import com.hrong.flink.model.KafkaData
import com.hrong.flink.utils.{AggFunction, ConsumerDeserializationSchema}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09


object WordCountStream {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.getConfig.disableSysoutLogging()
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(2)

    val topic = "flink"
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "s101")
    properties.setProperty("group.id", "flinkgroup")
    properties.setProperty("enable.auto.commit", "true")
    properties.setProperty("auto.commit.interval.ms", "5000")
    //自定义的source
    val consumer = new FlinkKafkaConsumer09(topic, new ConsumerDeserializationSchema(classOf[JSONObject]), properties)
    //从最新偏移量开始接受数据
    consumer.setStartFromLatest()
    // 添加自定义的source
    val messageStream = env.addSource(consumer)

    val kafkaDataStream = messageStream.map(item
    => KafkaData(item.getInteger("id"), item.getString("data"), item.getLong("time")))
    //指定数据源的时间戳
    val kafkaDataStreamWithTimeStamps = kafkaDataStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[KafkaData](Time.seconds(0L)) {
      override def extractTimestamp(element: KafkaData): Long = element.timeStamp
    })

    kafkaDataStreamWithTimeStamps
      .keyBy("id")
      //基于event_time的window
      .window(TumblingEventTimeWindows.of(Time.seconds(5L)))
      .allowedLateness(Time.seconds(5L))
      .aggregate(new AggFunction())
      .addSink(result => {
        println(result._1 + " countResult:" + result._2)
      })
    try {
      env.execute("kafkaJob")
    } catch {
      case _: Exception => println("出错啦")
    }
  }
}
