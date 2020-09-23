package com.hrong.flink.utils

import java.util.Date

import org.apache.commons.lang3.time.FastDateFormat
import org.slf4j.{Logger, LoggerFactory}


object DateUtil {

  val logger: Logger = LoggerFactory.getLogger("DateUtil")
  private val timeFormat: FastDateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")
  private val dtFormat: FastDateFormat = FastDateFormat.getInstance("yyyyMMdd")


  def now(): String = {
    timeFormat.format(new Date)
  }

  def today(): String = {
    dtFormat.format(new Date)
  }

  def getDt(time: Long): String = {
    val date = new Date(time)
    dtFormat.format(date)
  }

  def getTime(time: Long): String = {
    val date = new Date(time)
    timeFormat.format(date)
  }



}
