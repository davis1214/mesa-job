//package com.di.mesa.sparkjob.util
//
//
///**
//  * Created by Administrator on 17/7/3.
//  */
//object PatternTest {
//
//
//  def main(args: Array[String]) {
//
//
//    val line1 = """NOTICE: 2017-06-30 18:08:45.378 192.2.46.3 [Base.php:267 Base::printLog()] traceid[108c0000015cf87968e30a022e3b05ea,0] logid[149881732554085514] url[/wd/item/getRecommonItems?traceID=1ot4qnj4jp4xr5o6tt&scheme=https&param=%7B%22item_id%22:%222122559529%22,%22page%22:%220%22,%22limit%22:%226%22,%22seller_id%22:%22963259173%22,%22is_wx%22:%220%22%7D] req[{"getRecommonItems":"","traceID":"1ot4qnj4jp4xr5o6tt","scheme":"https","param":"{"item_id":"2122559529","page":"0","limit":"6","seller_id":"963259173","is_wx":"0"}"}] public[] loginfo[localip:192.2.46.59"|"module:item"|"remote_ip:"|"remote_port:"|"guid:"|"mem_call_num:1"|"tag:ol17904457261515106613"|"itemlist:2122556388#2022791609#2022793653#2022796489#2022795273#2022792730"|"item_id:2122559529"|"errno:0"|"errmsg:"|"real_ip:192.2.136.127 ] time[ single_db_conn:0 DataService_TraceLog:2 total:95 mem_get:2 t_redis_getm:2 t_redis_con:0 t_redis_con_redis_wd:0 db_query:7 db_query_vshop_r:7 db_con:3 db_con_wd_manage_r:1 db_con_wd_shop_r:1 t_wdsvr-item_0:33 DataService:2 ds_connect:0 t_getRecommonItems_0:20 db_con_vshop_r:1 t_market_0:20 ]"""
//    val line2 = """NOTICE: 2017-06-30 18:08:45.378 192.2.46.3 [Base.php:267 Base::printLog()] traceid[108c0000015cf87968e30a022e3b05ea,0] logid[149881732554085514] url[/wd/item?traceID=1ot4qnj4jp4xr5o6tt&scheme=https&param=%7B%22item_id%22:%222122559529%22,%22page%22:%220%22,%22limit%22:%226%22,%22seller_id%22:%22963259173%22,%22is_wx%22:%220%22%7D] req[{"getRecommonItems":"","traceID":"1ot4qnj4jp4xr5o6tt","scheme":"https","param":"{"item_id":"2122559529","page":"0","limit":"6","seller_id":"963259173","is_wx":"0"}"}] public[] loginfo[localip:192.2.46.59"|"module:item"|"remote_ip:"|"remote_port:"|"guid:"|"mem_call_num:1"|"tag:ol17904457261515106613"|"itemlist:2122556388#2022791609#2022793653#2022796489#2022795273#2022792730"|"item_id:2122559529"|"errno:0"|"errmsg:"|"real_ip:192.2.136.127 ] time[ single_db_conn:0 DataService_TraceLog:2 total:124 mem_get:2 t_redis_getm:2 t_redis_con:0 t_redis_con_redis_wd:0 db_query:7 db_query_vshop_r:7 db_con:3 db_con_wd_manage_r:1 db_con_wd_shop_r:1 t_wdsvr-item_0:33 DataService:2 ds_connect:0 t_getRecommonItems_0:20 db_con_vshop_r:1 t_market_0:20 ]"""
//    val line3 = """NOTICE: 2017-06-30 18:08:45.378 192.2.46.3 [Base.php:267 Base::printLog()] traceid[108c0000015cf87968e30a022e3b05ea,0] logid[149881732554085514] url[/wd/item/getRecommonItems/itemOder?traceID=1ot4qnj4jp4xr5o6tt&scheme=https&param=%7B%22item_id%22:%222122559529%22,%22page%22:%220%22,%22limit%22:%226%22,%22seller_id%22:%22963259173%22,%22is_wx%22:%220%22%7D] req[{"getRecommonItems":"","traceID":"1ot4qnj4jp4xr5o6tt","scheme":"https","param":"{"item_id":"2122559529","page":"0","limit":"6","seller_id":"963259173","is_wx":"0"}"}] public[] loginfo[localip:192.2.46.59"|"module:shop"|"remote_ip:"|"remote_port:"|"guid:"|"mem_call_num:1"|"tag:ol17904457261515106613"|"itemlist:2122556388#2022791609#2022793653#2022796489#2022795273#2022792730"|"item_id:2122559529"|"errno:0"|"errmsg:"|"real_ip:192.2.136.127 ] time[ single_db_conn:0 DataService_TraceLog:2 total:167 mem_get:2 t_redis_getm:2 t_redis_con:0 t_redis_con_redis_wd:0 db_query:7 db_query_vshop_r:7 db_con:3 db_con_wd_manage_r:1 db_con_wd_shop_r:1 t_wdsvr-item_0:33 DataService:2 ds_connect:0 t_getRecommonItems_0:20 db_con_vshop_r:1 t_market_0:20 ]"""
//    val line4 = """NOTICE: 2017-06-30 18:08:45.378 192.2.46.3 [Base.php:267 Base::printLog()] traceid[108c0000015cf87968e30a022e3b05ea,0] logid[149881732554085514] url[/wd/item/getRecommonItems/itemOder?traceID=1ot4qnj4jp4xr5o6tt&scheme=https&param=%7B%22item_id%22:%222122559529%22,%22page%22:%220%22,%22limit%22:%226%22,%22seller_id%22:%22963259173%22,%22is_wx%22:%220%22%7D] req[{"getRecommonItems":"","traceID":"1ot4qnj4jp4xr5o6tt","scheme":"https","param":"{"item_id":"2122559529","page":"0","limit":"6","seller_id":"963259173","is_wx":"0"}"}] public[] loginfo[localip:192.2.46.59"|"module:shop"|"remote_ip:"|"remote_port:"|"guid:"|"mem_call_num:1"|"tag:ol17904457261515106613"|"itemlist:2122556388#2022791609#2022793653#2022796489#2022795273#2022792730"|"item_id:2122559529"|"errno:0"|"errmsg:"|"real_ip:192.2.136.127 ] time[ single_db_conn:0 DataService_TraceLog:2 total:167 mem_get:2 t_redis_getm:2 t_redis_con:0 t_redis_con_redis_wd:0 db_query:7 db_query_vshop_r:7 db_con:3 db_con_wd_manage_r:1 db_con_wd_shop_r:1 t_wdsvr-item_0:33 DataService:2 ds_connect:0 t_getRecommonItems_0:20 db_con_vshop_r:1 t_market_0:20 ]"""
//    val line5 = """NOTICE: 2017-06-30 18:08:45.378 192.2.46.3 [Base.php:267 Base::printLog()] traceid[108c0000015cf87968e30a022e3b05ea,0] logid[149881732554085514] url[/wd/item/getRecommonItems/itemOder?traceID=1ot4qnj4jp4xr5o6tt&scheme=https&param=%7B%22item_id%22:%222122559529%22,%22page%22:%220%22,%22limit%22:%226%22,%22seller_id%22:%22963259173%22,%22is_wx%22:%220%22%7D] req[{"getRecommonItems":"","traceID":"1ot4qnj4jp4xr5o6tt","scheme":"https","param":"{"item_id":"2122559529","page":"0","limit":"6","seller_id":"963259173","is_wx":"0"}"}] public[] loginfo[localip:192.2.46.59"|"module:shop"|"remote_ip:"|"remote_port:"|"guid:"|"mem_call_num:1"|"tag:ol17904457261515106613"|"itemlist:2122556388#2022791609#2022793653#2022796489#2022795273#2022792730"|"item_id:2122559529"|"errno:0"|"errmsg:"|"real_ip:192.2.136.127 ] time[ single_db_conn:0 DataService_TraceLog:2 total:567 mem_get:2 t_redis_getm:2 t_redis_con:0 t_redis_con_redis_wd:0 db_query:7 db_query_vshop_r:7 db_con:3 db_con_wd_manage_r:1 db_con_wd_shop_r:1 t_wdsvr-item_0:33 DataService:2 ds_connect:0 t_getRecommonItems_0:20 db_con_vshop_r:1 t_market_0:20 ]"""
//    val line6 = """NOTICE: 2017-06-30 18:08:45.378 192.2.46.3 [Base.php:267 Base::printLog()] traceid[108c0000015cf87968e30a022e3b05ea,0] logid[149881732554085514] url[/wd/item/getRecommonItems/itemOder?traceID=1ot4qnj4jp4xr5o6tt&scheme=https&param=%7B%22item_id%22:%222122559529%22,%22page%22:%220%22,%22limit%22:%226%22,%22seller_id%22:%22963259173%22,%22is_wx%22:%220%22%7D] req[{"getRecommonItems":"","traceID":"1ot4qnj4jp4xr5o6tt","scheme":"https","param":"{"item_id":"2122559529","page":"0","limit":"6","seller_id":"963259173","is_wx":"0"}"}] public[] loginfo[localip:192.2.46.59"|"module:shop"|"remote_ip:"|"remote_port:"|"guid:"|"mem_call_num:1"|"tag:ol17904457261515106613"|"itemlist:2122556388#2022791609#2022793653#2022796489#2022795273#2022792730"|"item_id:2122559529"|"errno:0"|"errmsg:"|"real_ip:192.2.136.127 ] time[ single_db_conn:0 DataService_TraceLog:2 total:12 mem_get:2 t_redis_getm:2 t_redis_con:0 t_redis_con_redis_wd:0 db_query:7 db_query_vshop_r:7 db_con:3 db_con_wd_manage_r:1 db_con_wd_shop_r:1 t_wdsvr-item_0:33 DataService:2 ds_connect:0 t_getRecommonItems_0:20 db_con_vshop_r:1 t_market_0:20 ]"""
//
//
//    val lines = Array(line1, line2, line3, line4, line5, line6)
//
//    case class StrObject(url: String, module: String, total: Long)
//
//    //  url\[([^\]^\?]*)
//
//    val patternStr =
//    //"""NOTICE.*?total:({total}\d{1,})"""
//      """NOTICE.*?url\[({url}[^\]^\?]*).*module:({moduel}[^"]*).*total:({total}\d{1,}).*"""
//
//    //""""NOTICE.*?url\[({url}[^\]^?\]].*)\]" req.*total:({total}\d{1,}).*"""
//
//    val pattern = new Pattern(patternStr)
//
//
//
//
//    var i = 0
//    //"50ms":1000,"100ms":200,"200ms":100,"500ms":20,"1s":1
//    lines.map(line => {
//      val matcher: Matcher = pattern.matcher(line)
//
//      if (matcher.find())
//        StrObject(matcher.group("url"), matcher.group("moduel"), matcher.group("total").toLong)
//    }).foreach(f => {
//
//      if (f.isInstanceOf[StrObject]) {
//        println("-->" + f)
//      }
//
//    })
//
//
//    case class RequestTimeInfo(module: String, url: String, time: String)
//
//    case class Key1(module: String, url: String, span: String)
//    case class Key2(module: String, url: String)
//    case class Value(span: String, count: Long)
//
//    val a = lines.map(line => {
//      val matcher: Matcher = pattern.matcher(line)
//
//      if (matcher.find())
//        StrObject(matcher.group("url"), matcher.group("moduel"), matcher.group("total").toLong)
//    }).filter(f => f.isInstanceOf[StrObject])
//      .map(f = f => {
//        val strObjec: StrObject = f.asInstanceOf[StrObject]
//
//        // {"50ms":1000,"100ms":200,"200ms":100,"500ms":20,"1s":1}。
//        val span = if (strObjec.total <= 50) "50ms" else if (strObjec.total > 50 && strObjec.total <= 100) "100ms" else if (strObjec.total > 100 && strObjec.total <= 200) "200ms" else if (strObjec.total > 200 && strObjec.total <= 500) "500ms" else "1s"
//
//        (Key1(strObjec.module, strObjec.url, span), 1)
//      })
//      .groupBy(f => {
//        f._1
//      })
//
//
//    val aa = a.map(f => {
//      println("==>" + f._1  + " ," + f._2.toList)
//      (Key2(f._1.module, f._1.url),  f._2.length)
//    })
//
//
//    println(aa)
//    aa.foreach(println)
//
//    //    aa.groupBy(f => {
//    //      f._1
//    //    }).foreach(f => {
//    //      println(f)
//    //    })
//
//
//  }
//
//}
