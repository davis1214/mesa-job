package com.di.mesa.streamingjob.monitor.common

/**
 * Created by Administrator on 16/6/3.
 */
object TopicEnums extends Enumeration {
  type TopicEnums = Value

  val TEST = Value("test1")
  val APP_UDC_ACCOUNT = Value("app_udc_account")
  val PUBLIC = Value("public")

  def getValueByTopicEnumsId(id: Int) = TopicEnums(id).toString()
  def getValueByTopicEnumsIns(ins: TopicEnums) = TopicEnums(ins.id).toString()

}
