package common

/**
  * Created by Administrator on 16/6/3.
  */
object TopicEnums extends Enumeration {
  type TopicEnums = Value

  val TEST = Value(0, "test")
  val APP_UDC_ACCOUNT = Value("app_udc_account")
  val PUBLIC = Value("public")

}
