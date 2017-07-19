package com.di.mesa.job.jstorm.jobs.mq

/**
  * Created by Administrator on 17/7/11.
  */
object PatternDemo extends App {

  private val pattern1 = """NOTICE.*?url\[([^\]^\?]*).*module:([^"]*).*total:(\d{1,}).*""".r

  val line1 = """NOTICE: 2017-06-30 18:08:45.378 192.2.46.3 [Base.php:267 Base::printLog()] traceid[108c0000015cf87968e30a022e3b05ea,0] logid[149881732554085514] url[/wd/item/getRecommonItems?traceID=1ot4qnj4jp4xr5o6tt&scheme=https&param=%7B%22item_id%22:%222122559529%22,%22page%22:%220%22,%22limit%22:%226%22,%22seller_id%22:%22963259173%22,%22is_wx%22:%220%22%7D] req[{"getRecommonItems":"","traceID":"1ot4qnj4jp4xr5o6tt","scheme":"https","param":"{"item_id":"2122559529","page":"0","limit":"6","seller_id":"963259173","is_wx":"0"}"}] public[] loginfo[localip:192.2.46.59"|"module:item"|"remote_ip:"|"remote_port:"|"guid:"|"mem_call_num:1"|"tag:ol17904457261515106613"|"itemlist:2122556388#2022791609#2022793653#2022796489#2022795273#2022792730"|"item_id:2122559529"|"errno:0"|"errmsg:"|"real_ip:192.2.136.127 ] time[ single_db_conn:0 DataService_TraceLog:2 total:95 mem_get:2 t_redis_getm:2 t_redis_con:0 t_redis_con_redis_wd:0 db_query:7 db_query_vshop_r:7 db_con:3 db_con_wd_manage_r:1 db_con_wd_shop_r:1 t_wdsvr-item_0:33 DataService:2 ds_connect:0 t_getRecommonItems_0:20 db_con_vshop_r:1 t_market_0:20 ]"""

  val str1 = line1 match {
    case pattern1(url, module, userId) => println(url + "->" + module + "->" + userId.toLong)
    case _ => println("no match result")
  }


  val logPattern =
    """"requestTime:(\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d).*requesturl:([^\]^\?]*).*userID:(\d{1,})"""

  val logPattern2 = """"requestTime:(\d{4}-\d{2}-\d{2})""".r


  val log =
    """"requestTime:2017-06-28 11:00:00"|"requestTime_ms:2017-06-28 11:00:00 033"|"requesturl:http://gw.api.mesa.com/app/797/wd/pb/push/upload_push_info"|"x-real-ip:175.42.201.220"|"reqMethod:POST"|"scheme:https"|"X-Real-Port:51141"|"remote_port:51141"|"remote_addr:192.2.100.37"|"http_x_forwarded_for:175.42.201.220"|"traceID:62wfpj4gexuox22ca"|"open_id:E407938D-A531-4C51-929C-DD5A60C3CAF5"|"proxysign:52C17B37A3594998"|"notifyStatus:1"|"channel:1000f"|"userScene:3"|"sessionid:ks_1_1498618789286_893935_100"|"userID:1192277762"|"machineName:iPhone8,2"|"wduss:7605292c31baec5c97fcd20799859a795451647914114f6ee18926a733244f12a60b0649f357ba9f94d4962facd47628"|"net_timestamp_type:server"|"idfv:E756CD74-902A-47A2-AECA-1924ECED74BD"|"brand:Apple"|"lat:25.1163465050098"|"device_id:114882375"|"idfa:8E4C598D-ED6A-41A4-AA87-821AC6966509"|"alt:375.4355163574219"|"keyid:1.1.1"|"appstatus:background"|"version:7.9.7"|"signid:4.1"|"wssid:FAST_BC5A"|"access_token:AAAAAcI/in2tH0jtt51/hVgIz/5YKLD3IcO4eU4U0jKltpbVC97kMSQBZEwjglfKScIJIFwneWlKDrkIATlRXEs47s93UB6g1fItP1ei93E+f+6EXuNUpu/Nj7KpzieFZR5LMsQy2lBpMBt2cr1KFBl3Bgg."|"shop_id:1192277762"|"apiv:797"|"guid:1498480929307_3227642"|"loc:117.0213689838664,25.1163465050098,375.4355163574219"|"userStatus:1"|"kid:1.1.1"|"bundleid:com.mesa.mesa"|"mid:iPhone"|"lon:117.0213689838664"|"imsi:"|"mac:02:00:00:00:00:00"|"platform:iphone"|"openudid:355e32fe6fdad166eca912474873104603caf40e"|"network:WIFI"|"wmac:78:eb:14:4a:bc:5a"|"encryType:2"|"os:9.3"|"gzipType:1"|"pushTypes:[{\"pushStatus\":\"1\",\"pushType\":\"2\",\"token\":\"782d337e814ae51dc73be53abcf4f91669fa88b3e3b5d58590993257813b6097\",\"refreshed\":\"0\"},{\"pushStatus\":\"1\",\"pushType\":\"20\",\"token\":\"\",\"refreshed\":\"0\"}]"|"h:2208"|"token:782d337e814ae51dc73be53abcf4f91669fa88b3e3b5d58590993257813b6097"|"device_id_v2:6eef4802b89f6df6e29417b3244a1294+89f9991039df6a90cd0972d8071e173e"|"build:20170620200317"|"appid:com.mesa.weishop"|"w:1242"|"netsubtype:"|"imei:"|"net_timestamp:1498618799997"|"wfr:null"|"real_apiv:0"|"rmethod:wd/pb/push/upload_push_info"|"executeTime:84" |"""


  val str = log match {
    //case pattern(requestTime, requestUrl, userId) => (requestTime, requestUrl, userId.toLong)
    case logPattern2(requestTime) => println("requestTime -> " + requestTime)
    case _ => println("no match result")
  }


  logPattern2.findAllIn(log) match {
    case a => println(a)
    case _ => println("0000>")
  }


  println("--< " + str)


}
