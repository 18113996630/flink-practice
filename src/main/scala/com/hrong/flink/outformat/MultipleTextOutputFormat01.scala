package com.hrong.flink.outformat

import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat

class MultipleTextOutputFormat01[K, V] extends MultipleTextOutputFormat[K, V] {

  override def generateFileNameForKeyValue(key: K, value: V, name: String): String = key.asInstanceOf[String]

  override def generateActualKey(key: K, value: V): K = NullWritable.get().asInstanceOf[K]

  override def generateActualValue(key: K, value: V): V = value.asInstanceOf[V]
}
