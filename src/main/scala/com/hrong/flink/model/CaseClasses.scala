package com.hrong.flink.model

case class StudentScala(id: Int, class_id: Int, name: String, age: Int) {
  override def toString: String = "StudentJava{" + "id=" + id + ", classId=" + class_id + ", name='" + name + '\'' + ", age=" + age + '}'
}

case class IncomeWithEventTime(storedId: Int, money: Double, time: Long)

case class Income(storeId: Int, money: Double)


case class FruitsPrice(name: String, unitPrice: Double) {
  override def toString: String = name + ":" + unitPrice
}

case class Cost(name: String, money: Double)

case class WeightInfo(name: String, weight: Double) {
  override def toString: String = name + ":" + weight
}

case class LineCount(line: String, count: Int) {
  override def toString: String = line + " " + count
}

case class people(id: Int, name: String, age: Int)

case class WordCount(word: String, count: Int)

case class KafkaData(id: Integer, data: String, timeStamp: Long)
