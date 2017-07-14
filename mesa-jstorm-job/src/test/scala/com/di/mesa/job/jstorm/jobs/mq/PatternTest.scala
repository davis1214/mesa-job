package com.di.mesa.job.jstorm.jobs.mq

import scala.util.matching._


/**
  * Created by Administrator on 17/6/29.
  */
object PatternTest {


  def main(args: Array[String]) {


    val log =
      """"requestTime:2017-06-28 11:00:00"|"requestTime_ms:2017-06-28 11:00:00 033"|"requesturl:http://gw.api.mesa
        |.com/app/797/wd/pb/push/upload_push_info"|"x-real-ip:175.42.201.220"|"reqMethod:POST"|"scheme:https"|"X-Real-Port:51141"|"remote_port:51141"|"remote_addr:10.2.100.37"|"http_x_forwarded_for:175.42.201.220"|"traceID:62wfpj4gexuox22ca"|"open_id:E407938D-A531-4C51-929C-DD5A60C3CAF5"|"proxysign:52C17B37A3594998"|"notifyStatus:1"|"channel:1000f"|"userScene:3"|"sessionid:ks_1_1498618789286_893935_100"|"userID:1192277762"|"machineName:iPhone8,2"|"wduss:7605292c31baec5c97fcd20799859a795451647914114f6ee18926a733244f12a60b0649f357ba9f94d4962facd47628"|"net_timestamp_type:server"|"idfv:E756CD74-902A-47A2-AECA-1924ECED74BD"|"brand:Apple"|"lat:25.1163465050098"|"device_id:114882375"|"idfa:8E4C598D-ED6A-41A4-AA87-821AC6966509"|"alt:375.4355163574219"|"keyid:1.1.1"|"appstatus:background"|"version:7.9.7"|"signid:4.1"|"wssid:FAST_BC5A"|"access_token:AAAAAcI/in2tH0jtt51/hVgIz/5YKLD3IcO4eU4U0jKltpbVC97kMSQBZEwjglfKScIJIFwneWlKDrkIATlRXEs47s93UB6g1fItP1ei93E+f+6EXuNUpu/Nj7KpzieFZR5LMsQy2lBpMBt2cr1KFBl3Bgg."|"shop_id:1192277762"|"apiv:797"|"guid:1498480929307_3227642"|"loc:117.0213689838664,25.1163465050098,375.4355163574219"|"userStatus:1"|"kid:1.1.1"|"bundleid:com.mesa.mesa"|"mid:iPhone"|"lon:117.0213689838664"|"imsi:"|"mac:02:00:00:00:00:00"|"platform:iphone"|"openudid:355e32fe6fdad166eca912474873104603caf40e"|"network:WIFI"|"wmac:78:eb:14:4a:bc:5a"|"encryType:2"|"os:9.3"|"gzipType:1"|"pushTypes:[{\"pushStatus\":\"1\",\"pushType\":\"2\",\"token\":\"782d337e814ae51dc73be53abcf4f91669fa88b3e3b5d58590993257813b6097\",\"refreshed\":\"0\"},{\"pushStatus\":\"1\",\"pushType\":\"20\",\"token\":\"\",\"refreshed\":\"0\"}]"|"h:2208"|"token:782d337e814ae51dc73be53abcf4f91669fa88b3e3b5d58590993257813b6097"|"device_id_v2:6eef4802b89f6df6e29417b3244a1294+89f9991039df6a90cd0972d8071e173e"|"build:20170620200317"|"appid:com.mesa.weishop"|"w:1242"|"netsubtype:"|"imei:"|"net_timestamp:1498618799997"|"wfr:null"|"real_apiv:0"|"rmethod:wd/pb/push/upload_push_info"|"executeTime:84"""".stripMargin



    //"requestTime:2017-06-28 11:00:00"|".*"|"requesturl:http://gw.api.mesa|.*?|"userID:1192277762"|"machineName:


    // val log: String = """"requestTime:2017-06-28 11:00:00"|"requestTime_ms:2017-06-28 11:00:00 033"|"""
    // val logPattern = """"requestTime:(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})"|"requestTime_ms.*?"""
    val logPattern =
      """"requestTime:(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})"|"requestTime_ms:((\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{3})).*?"""

    val regex1 = new Regex(logPattern)

    // val regex1(time) = log
    // println("--->" + time)


    //TODO test1
    logPattern.r.findFirstIn(log) match {
      case Some(time) => println("-->" + time)
      case _ => println("nothing")
    }


    logPattern.r.findAllMatchIn(log).foreach(x => {
      println("x===>" + x)
    })


    //TODO 测试2
    val rr =
      """"requestTime:(\\w*)\"|\"requestTime_ms:\w*"""
    //val rr = """"requestTime:({requestTime}\\w*)\"|\"requestTime_ms:\w*"""

    rr.r.findFirstIn(log) match {
      case Some(time) => println("rr-->" + time)
      case _ => println("nothing")
    }




    val dateP1 = new scala.util.matching.Regex("""(\d\d\d\d)-(\d\d)-(\d\d)""", "year", "month", "day")

    val terday = "terday is 2016-18-12"
    val lst = dateP1 findAllIn terday toList

    lst.foreach(f => println("find all in " + f))


    val terday2 = "terday is 2016-18-12 19:12:11"
    val dateP2 = new scala.util.matching.Regex("""(\d\d\d\d)-(\d\d)-(\d\d) 19:12:11""")

    val lst2 = dateP2.findAllIn(terday2)

    println("new2 : " + lst2.mkString("->"))
    println("new2 findPrefixOf : " + dateP2.findPrefixOf("year"))



    val dateP3 = new scala.util.matching.Regex(""""requestTime:(\d\d\d\d)-(\d\d)-(\d\d) (\d\d):(\d\d):(\d\d).*"|"requesturl:(\w*)|""")

    val lst3 = dateP3.findAllIn(log)

    //println("new3 : " + lst3.mkString("->"))
    //println("new3 findPrefixOf : " + dateP2.findPrefixOf("year"))









    //
    //    val p = s^requestTime:(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})"|"requestTime_ms"""
    //
    //
    //    // url\[([^(\?|\])]+).*?\]    //&&&info\[([^(\?|\])]+).*?\]
    //
    //
    //
    //    p.r.findFirstIn(log)match {
    //      case Some(time) =>println("--->".concat(time))
    //
    //      case _ =>println("no data!!!!")
    //    }


    //test1


    //
    //    val ll = s"""requestTime:2017-06-28 11:00:00"|"requestTime_ms:2017-06-28"""
    //
    //
    //    val rr = new Regex("""requestTime:(.*)"|"requestTime_ms:2017-06-28""")
    //
    //
    //    val rr(time) = ll
    //    println("--> " + ll)

  }

  def test1: Unit = {
    val regex = new Regex("""([0-9]+) ([a-z]+)""")
    val line = "123 iteblog"
    val regex(num, blog) = line

    println(num + "  , " + blog)
  }
}
