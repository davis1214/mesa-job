package com.di.mesa.streamingjob

import common.TopicEnums
import common.TopicEnums._


/**
  * Created by Administrator on 16/5/17.
  */
object demo extends App {


  println("test")

  def doWhat2(topic: TopicEnums) = topic match {
    case APP_UDC_ACCOUNT => "app_udc_account"
    case TEST => "test"
    case _ => "go"
  }


  def doWhat1(topic: String) = TopicEnums.withName(topic) match {
    case APP_UDC_ACCOUNT => "stop"
    case TEST => "test"
    case _ => "go"
  }

  println("-->"+doWhat2(TopicEnums.TEST)+ " ," + PUBLIC.id)

  val appid = "application_1459494992574_26799"

  println(s"app id is $appid")

  println("--->"+ doWhat1("app_udc_account"))

  
  
  
  println(TopicEnums(TopicEnums.APP_UDC_ACCOUNT.id))
  
}
