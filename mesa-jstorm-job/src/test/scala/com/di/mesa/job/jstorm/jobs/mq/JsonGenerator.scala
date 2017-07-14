package com.di.mesa.job.jstorm.jobs.mq

import com.google.gson.GsonBuilder

import scala.collection.mutable.ArrayBuffer

/**
  * Created by Administrator on 17/6/28.
  */
object JsonGenerator extends App {


  case class UserAction(name: String, time: String)


  //{"userId":"1192277762","activeTime":"2017-06-28 15:00:00"}
  // val users = Array(UserAction("1192277762_1", "2017-06-28 15:00:01"), UserAction("1192277762_2", "2017-06-28 15:00:02"), UserAction("1192277762_3", "2017-06-28 15:00:03"))
  val users = Array(UserAction("1192277762_1", "2017-06-28 15:00:01"), UserAction("1192277762_2", "2017-06-28 15:00:02"), UserAction("1192277762_3", "2017-06-28 15:00:03"))

  var objectArray = new ArrayBuffer[UserAction](100)
  objectArray += (UserAction("1192277762_11", "2017-06-28 15:00:01"), UserAction("1192277762_22", "2017-06-28 15:00:02"), UserAction("1192277762_33", "2017-06-28 15:00:03"))





  //objectArray += UserAction("1192277762_11", "2017-06-28 15:00:01")

  val builder = new GsonBuilder();
  val mapper = builder.create();


  // [{"name":"1192277762_1","time":"2017-06-28 15:00:01"},{"name":"1192277762_2","time":"2017-06-28 15:00:02"},{"name":"1192277762_3","time":"2017-06-28 15:00:03"}]

  println(mapper.toJson(users))

  println(mapper.toJson(objectArray.toArray))

}
