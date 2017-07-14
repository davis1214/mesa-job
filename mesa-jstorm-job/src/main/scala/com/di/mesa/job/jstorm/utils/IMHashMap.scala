package com.di.mesa.job.jstorm.utils

import org.joda.time.format.DateTimeFormat

/**
  * Created by Administrator on 17/6/29.
  */
class IMHashMap {


  private val dateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")

  var map = scala.collection.mutable.HashMap[String, String]()

  def update(userId: String, requestTime: String): Unit = {
    if (!map.keySet.contains(userId)) {
      map += (userId -> requestTime)
    }
    else {
      val lastRequestTime = map(userId)
      val lasetRequestDateTime = dateTimeFormatter.parseDateTime(lastRequestTime).getMillis
      val currentRequestDateTime = dateTimeFormatter.parseDateTime(requestTime).getMillis

      if (currentRequestDateTime > lasetRequestDateTime) map += (userId -> requestTime)

    }
  }


  def getMap: scala.collection.mutable.HashMap[String, String] = map


  def test: Unit = {
    val readableTime = "2017-08-04 11:12:14"
    val dateTime = dateTimeFormatter.parseDateTime(readableTime)

    println("timestamp:" + dateTime)

    update("aaa", readableTime)

    println("----->")

    map.foreach(println)
  }

}
