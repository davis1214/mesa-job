package com.di.mesa.sparkjob.collections

/**
  * Created by Administrator on 17/7/6.
  */
object GroupByDemo {

  def main(args: Array[String]) {


    case class Key1(module: String, url: String, span: String)
    case class Key2(module: String, url: String)
    case class RequestTimeInfo(mode: String, url: String, requestCount: Long, timeCount: String, addTime: Long, updateTime: Long)




//    val a = Array((Key1(wdm,/wdm/shop/setDistributor,50ms),3),(Key1(wdm,/wdm/shop/setDistributor,50ms),3),(Key1(wdm,/wdm/shop/setDistributor,50ms),3),(Key1(wdm,
//      /wdm/shop/setDistributor,50ms),3),(Key1(wdm,/wdm/shop/setDistributor,50ms),2),(Key1(wdm,/wdm/shop/setDistributor,50ms),1),(Key1(wdm,/wdm/shop/setDistributor,50ms),2),(Key1(wdm,/wdm/shop/setDistributor,50ms),1),(Key1(wdm,/wdm/shop/setDistributor,50ms),3),(Key1(wdm,/wdm/shop/setDistributor,50ms),1),(Key1(wdm,/wdm/shop/setDistributor,50ms),2),(Key1(wdm,/wdm/shop/setDistributor,50ms),6),(Key1(wdm,/wdm/shop/setDistributor,50ms),3),(Key1(wdm,/wdm/shop/setDistributor,50ms),2),(Key1(wdm,/wdm/shop/setDistributor,50ms),3),(Key1(wdm,/wdm/shop/setDistributor,50ms),1),(Key1(wdm,/wdm/shop/setDistributor,50ms),3),(Key1(wdm,/wdm/shop/setDistributor,50ms),1),(Key1(wdm,/wdm/shop/setDistributor,50ms),3),(Key1(wdm,/wdm/shop/setDistributor,50ms),3),(Key1(wdm,/wdm/shop/setDistributor,50ms),2),(Key1(wdm,/wdm/shop/setDistributor,50ms),3),(Key1(wdm,/wdm/shop/setDistributor,50ms),4),(Key1(wdm,/wdm/shop/setDistributor,50ms),3),(Key1(wdm,/wdm/shop/setDistributor,50ms),3),(Key1(wdm,/wdm/shop/setDistributor,50ms),2),(Key1(wdm,/wdm/shop/setDistributor,50ms),1),(Key1(wdm,/wdm/shop/setDistributor,50ms),1),(Key1(wdm,/wdm/shop/setDistributor,50ms),3),(Key1(wdm,/wdm/shop/setDistributor,50ms),2),(Key1(wdm,/wdm/shop/setDistributor,50ms),2),(Key1(wdm,/wdm/shop/setDistributor,50ms),3),(Key1(wdm,/wdm/shop/setDistributor,50ms),3),(Key1(wdm,/wdm/shop/setDistributor,50ms),3),(Key1(wdm,/wdm/shop/setDistributor,50ms),4),(Key1(wdm,/wdm/shop/setDistributor,50ms),3),(Key1(wdm,/wdm/shop/setDistributor,50ms),2),(Key1(wdm,/wdm/shop/setDistributor,50ms),3),(Key1(wdm,/wdm/shop/setDistributor,50ms),1),(Key1(wdm,/wdm/shop/setDistributor,50ms),2),(Key1(wdm,/wdm/shop/setDistributor,50ms),6),(Key1(wdm,/wdm/shop/setDistributor,50ms),2),(Key1(wdm,/wdm/shop/setDistributor,50ms),2),(Key1(wdm,/wdm/shop/setDistributor,50ms),5),(Key1(wdm,/wdm/shop/setDistributor,50ms),1),(Key1(wdm,/wdm/shop/setDistributor,50ms),1),(Key1(wdm,/wdm/shop/setDistributor,50ms),2),(Key1(wdm,/wdm/shop/setDistributor,50ms),4),(Key1(wdm,/wdm/shop/setDistributor,50ms),1),(Key1(wdm,/wdm/shop/setDistributor,50ms),3),(Key1(wdm,/wdm/shop/setDistributor,50ms),1),(Key1(wdm,/wdm/shop/setDistributor,50ms),3),(Key1(wdm,/wdm/shop/setDistributor,50ms),4),(Key1(wdm,/wdm/shop/setDistributor,50ms),1),(Key1(wdm,/wdm/shop/setDistributor,50ms),4),(Key1(wdm,/wdm/shop/setDistributor,50ms),1),(Key1(wdm,/wdm/shop/setDistributor,50ms),3),(Key1(wdm,/wdm/shop/setDistributor,50ms),1),(Key1(wdm,/wdm/shop/setDistributor,50ms),1),(Key1(wdm,/wdm/shop/setDistributor,50ms),2),(Key1(wdm,/wdm/shop/setDistributor,50ms),2),(Key1(wdm,/wdm/shop/setDistributor,50ms),2),(Key1(wdm,/wdm/shop/setDistributor,50ms),1),(Key1(wdm,/wdm/shop/setDistributor,50ms),1),(Key1(wdm,/wdm/shop/setDistributor,50ms),2),(Key1(wdm,/wdm/shop/setDistributor,50ms),3),(Key1(wdm,/wdm/shop/setDistributor,50ms),1),(Key1(wdm,/wdm/shop/setDistributor,50ms),3),(Key1(wdm,/wdm/shop/setDistributor,50ms),3),(Key1(wdm,/wdm/shop/setDistributor,50ms),1),(Key1(wdm,/wdm/shop/setDistributor,50ms),2),(Key1(wdm,/wdm/shop/setDistributor,50ms),1),(Key1(wdm,/wdm/shop/setDistributor,50ms),1),(Key1(wdm,/wdm/shop/setDistributor,50ms),1),(Key1(wdm,/wdm/shop/setDistributor,50ms),4),(Key1(wdm,/wdm/shop/setDistributor,50ms),2),(Key1(wdm,/wdm/shop/setDistributor,50ms),2),(Key1(wdm,/wdm/shop/setDistributor,50ms),3),(Key1(wdm,/wdm/shop/setDistributor,50ms),3),(Key1(wdm,/wdm/shop/setDistributor,50ms),3),(Key1(wdm,/wdm/shop/setDistributor,50ms),4),(Key1(wdm,/wdm/shop/setDistributor,50ms),1),(Key1(wdm,/wdm/shop/setDistributor,50ms),2),(Key1(wdm,/wdm/shop/setDistributor,50ms),3),(Key1(wdm,/wdm/shop/setDistributor,50ms),3),(Key1(wdm,/wdm/shop/setDistributor,50ms),3),(Key1(wdm,/wdm/shop/setDistributor,50ms),3),(Key1(wdm,/wdm/shop/setDistributor,50ms),2),(Key1(wdm,/wdm/shop/setDistributor,50ms),2),(Key1(wdm,/wdm/shop/setDistributor,50ms),1),(Key1(wdm,/wdm/shop/setDistributor,50ms),4),(Key1(wdm,/wdm/shop/setDistributor,50ms),1),(Key1(wdm,/wdm/shop/setDistributor,50ms),1),(Key1(wdm,/wdm/shop/setDistributor,50ms),1),(Key1(wdm,/wdm/shop/setDistributor,50ms),1),(Key1(wdm,/wdm/shop/setDistributor,50ms),2),(Key1(wdm,/wdm/shop/setDistributor,50ms),2),(Key1(wdm,/wdm/shop/setDistributor,50ms),2),(Key1(wdm,/wdm/shop/setDistributor,50ms),3),(Key1(wdm,/wdm/shop/setDistributor,50ms),1))

    val a = Array((Key1("wdm","/wdm/shop/setDistributor","50ms"),3),(Key1("wdm","/wdm/shop/setDistributor","50ms"),2),(Key1("wdm","/wdm/shop/setDistributor","50ms"),1),(Key1
    ("wdm","/wdm/shop/setDistributor2","50ms"),3))

    println(a.length)

    a.groupBy(f=>f._1).foreach(f=>{

      var totalCount = 0
      f._2.foreach(x=>{
        totalCount += x._2
      })

      println(f._1 + "->"+ totalCount)


    })


    val ss = "msgagent,/udc/mq/rabbitmq/shop_wx_follow_push,50ms"

    val Array(module,url,span) = "msgagent,/udc/mq/rabbitmq/shop_wx_follow_push,50ms".split(",")

    println("-->")
    println(module+ " , " + url)
    println("-->2")


  }




}

