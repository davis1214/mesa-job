package com.di.mesa.sparkjob.parser

import java.sql._

import com.di.mesa.sparkjob.util.{DateUtil, SparkCommon}
import jregex.{Matcher, Pattern}
import org.apache.spark.rdd.RDD
import org.json4s.JsonDSL._
import org.json4s._
import org.json4s.jackson.JsonMethods._

import scala.collection.mutable.ArrayBuffer

/**
  * Created by Administrator on 17/7/3.
  */
object UrlCostsStatOne {


  implicit def string2boolean(arg: String): Boolean = arg.toBoolean

  private val pattern = new Pattern("""NOTICE.*?url\[({url}[^\]^\?]*).*module:({module}[^"]*).*total:({total}\d{1,}).*""")

  //case class StrObject(url: String, module: String, total: Long)
  //case class RequestTimeInfo(mode: String, url: String, requestCount: Long, timeCount: String, addTime: Long, updateTime: Long)


  def main(args: scala.Array[String]) {

    val isLocalMode: Boolean = if (args.length > 0) args(0) else "true"

    val datestr = if (args.length > 1) args(1) else DateUtil.getDayOfMonth - 1

    val isTest: Boolean = if (args.length > 2) args(2) else "true"

    //clear data
    clearHistoryData(isLocalMode)

    val sourceRdd: RDD[String] = initSourceRdd(isLocalMode, isTest)

    val parsedRdd = sourceRdd
      .map(line => {
        val matcher: Matcher = pattern.matcher(line);

        if (matcher.find()) {
          val url = matcher.group("url")
          val module = matcher.group("module")
          val total = matcher.group("total")

          val time = total.toInt
          val span = if (time <= 50) "50ms"
          else if (time > 50 && time <= 100) "100ms"
          else if (time > 100 && time <= 200) "200ms"
          else if (time > 200 && time <= 500) "500ms"
          else "1s"

          val key1 = module.concat("@@").concat(url).concat("@@").concat(span)

          (key1, 1)
        } else {
          ("-1", 1)
        }
      })
      .filter(f => {
        !f._1.startsWith("-1") && !f._1.startsWith("/wd/weixin/callback/")
        //}).map(f :(String,String ,Long)=> {
      })
      .reduceByKey(_ + _, 10)


    // 处理后, module@@url@@span
    val parsedAndFilteredRdd = parsedRdd.map(f => {
      try {
        val key1 = f._1.split("@@")
        val key2 = key1(0).concat("@@").concat(key1(1))
        (key2, (key1(2), f._2))
      } catch {
        case e: Exception => ("-1", ("0ms", f._2))
      }
    }).filter(f => !f._1.startsWith("-1")
    )


    //(module@@url ,(span, count))
    //(key2, (key1(2), f._2))

    val objectParsedRdd = parsedAndFilteredRdd
      .groupBy(f => f._1)
      .map(f => {
        var totalRequestCount: Int = 0
        var metrics = ArrayBuffer[JsonAST.JValue]()

        f._2.foreach(s => {
          totalRequestCount += s._2._2
          metrics += (s._2._1 -> s._2._2)
        })

        val timeCount = compact(render(metrics))
        try {
          val key2 = f._1.split("@@")

          //(mode: String, url: String, requestCount: Long, timeCount: String, addTime: Long, updateTime: Long)
          (key2(0), key2(1), totalRequestCount, timeCount, System.currentTimeMillis(), System.currentTimeMillis())
        } catch {
          case e: Exception => ("-1", "-1", totalRequestCount, timeCount, System.currentTimeMillis(), System.currentTimeMillis())
        }
      }).filter(f => !f._1.equals("-1"))


    def mysqlPersistFunc: (Iterator[(String, String, Int, String, Long, Long)]) => Unit = {
      iter => {

        val tableName = s"requset_time_info"
        val insertSql = s"insert into $tableName (url, mode, request_count,time_count,add_time ,update_time,log_date) values (?, ?, ?, ?, ?, ?, ?) ;"

        var conn: Connection = null
        val d: Driver = null
        var pstmt: PreparedStatement = null

        try {
          val (url: String, user: String, password: String) = getDBConfig(isLocalMode)
          conn = DriverManager.getConnection(url, user, password)

          while (iter.hasNext) {
            val item = iter.next()
            pstmt = conn.prepareStatement(insertSql);

            //(mode: String, url: String, requestCount: Long, timeCount: String, addTime: Long, updateTime: Long)
            pstmt.setString(1, item._2) //url
            pstmt.setString(2, item._1) //mode
            pstmt.setInt(3, item._3) //request_count
            pstmt.setString(4, item._4) //timeCount
            pstmt.setTimestamp(5, new Timestamp(item._5)) //addTime
            pstmt.setTimestamp(6, new Timestamp(item._6)) //updateTime
            pstmt.setDate(7, new Date(DateUtil.getlastDay.getMillis))
            pstmt.executeUpdate();
          }

        } catch {
          case e: Exception => e.printStackTrace()
        } finally {
          //if (ps != null) ps.close()
          if (pstmt != null) pstmt.close()
          if (conn != null) conn.close()
        }
      }
    }

    objectParsedRdd.repartition(2).foreachPartition(mysqlPersistFunc)


  }


  def getDBConfig(isLocalMode: Boolean): (String, String, String) = {
    if (!isLocalMode) {

      val url = "jdbc:mysql://localhost:3306/mesa_report";
      val user = "mesa_report";
      val password = "mesa_report"

      (url, user, password)
    } else {
      val url = "jdbc:mysql://localhost:3306/test";
      val user = "root";
      val password = "root"
      (url, user, password)
    }
  }


  def clearHistoryData(isLocalMode: Boolean): Unit = {

    val start = System.currentTimeMillis()
    println("start to clear history data in mysql")
    val lastDayFmt = DateUtil.getlastDayFmt
    val tableName = s"requset_time_info"
    val deleteSql = s"delete from $tableName where log_date = '${lastDayFmt}' ;"

    var conn: Connection = null
    val d: Driver = null
    var ps: PreparedStatement = null

    try {
      val (url: String, user: String, password: String) = getDBConfig(isLocalMode)
      conn = DriverManager.getConnection(url, user, password)
      ps = conn.prepareStatement(deleteSql)
      ps.execute()
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      if (ps != null) ps.close()
      if (conn != null) conn.close()
    }

    println("clear data costs " + (System.currentTimeMillis() - start))
  }

  def initSourceRdd(isLocalMode: Boolean, isTest: Boolean): RDD[String] = {

    val sparkContext = if (isLocalMode) SparkCommon.getLocalSparkContext
    else SparkCommon.getClusterSparkContext


    if (isLocalMode) {
      val line1 = """NOTICE: 2017-06-30 18:08:45.378 192.2.46.3 [Base.php:267 Base::printLog()] traceid[108c0000015cf87968e30a022e3b05ea,0] logid[149881732554085514] url[/wd/item/getRecommonItems?traceID=1ot4qnj4jp4xr5o6tt&scheme=https&param=%7B%22item_id%22:%222122559529%22,%22page%22:%220%22,%22limit%22:%226%22,%22seller_id%22:%22963259173%22,%22is_wx%22:%220%22%7D] req[{"getRecommonItems":"","traceID":"1ot4qnj4jp4xr5o6tt","scheme":"https","param":"{"item_id":"2122559529","page":"0","limit":"6","seller_id":"963259173","is_wx":"0"}"}] public[] loginfo[localip:192.2.46.59"|"module:item"|"remote_ip:"|"remote_port:"|"guid:"|"mem_call_num:1"|"tag:ol17904457261515106613"|"itemlist:2122556388#2022791609#2022793653#2022796489#2022795273#2022792730"|"item_id:2122559529"|"errno:0"|"errmsg:"|"real_ip:192.2.136.127 ] time[ single_db_conn:0 DataService_TraceLog:2 total:95 mem_get:2 t_redis_getm:2 t_redis_con:0 t_redis_con_redis_wd:0 db_query:7 db_query_vshop_r:7 db_con:3 db_con_wd_manage_r:1 db_con_wd_shop_r:1 t_wdsvr-item_0:33 DataService:2 ds_connect:0 t_getRecommonItems_0:20 db_con_vshop_r:1 t_market_0:20 ]"""
      val line2 = """NOTICE: 2017-06-30 18:08:45.378 192.2.46.3 [Base.php:267 Base::printLog()] traceid[108c0000015cf87968e30a022e3b05ea,0] logid[149881732554085514] url[/wd/item?traceID=1ot4qnj4jp4xr5o6tt&scheme=https&param=%7B%22item_id%22:%222122559529%22,%22page%22:%220%22,%22limit%22:%226%22,%22seller_id%22:%22963259173%22,%22is_wx%22:%220%22%7D] req[{"getRecommonItems":"","traceID":"1ot4qnj4jp4xr5o6tt","scheme":"https","param":"{"item_id":"2122559529","page":"0","limit":"6","seller_id":"963259173","is_wx":"0"}"}] public[] loginfo[localip:192.2.46.59"|"module:item"|"remote_ip:"|"remote_port:"|"guid:"|"mem_call_num:1"|"tag:ol17904457261515106613"|"itemlist:2122556388#2022791609#2022793653#2022796489#2022795273#2022792730"|"item_id:2122559529"|"errno:0"|"errmsg:"|"real_ip:192.2.136.127 ] time[ single_db_conn:0 DataService_TraceLog:2 total:124 mem_get:2 t_redis_getm:2 t_redis_con:0 t_redis_con_redis_wd:0 db_query:7 db_query_vshop_r:7 db_con:3 db_con_wd_manage_r:1 db_con_wd_shop_r:1 t_wdsvr-item_0:33 DataService:2 ds_connect:0 t_getRecommonItems_0:20 db_con_vshop_r:1 t_market_0:20 ]"""
      val line3 = """NOTICE: 2017-06-30 18:08:45.378 192.2.46.3 [Base.php:267 Base::printLog()] traceid[108c0000015cf87968e30a022e3b05ea,0] logid[149881732554085514] url[/wd/item/getRecommonItems/itemOder?traceID=1ot4qnj4jp4xr5o6tt&scheme=https&param=%7B%22item_id%22:%222122559529%22,%22page%22:%220%22,%22limit%22:%226%22,%22seller_id%22:%22963259173%22,%22is_wx%22:%220%22%7D] req[{"getRecommonItems":"","traceID":"1ot4qnj4jp4xr5o6tt","scheme":"https","param":"{"item_id":"2122559529","page":"0","limit":"6","seller_id":"963259173","is_wx":"0"}"}] public[] loginfo[localip:192.2.46.59"|"module:shop"|"remote_ip:"|"remote_port:"|"guid:"|"mem_call_num:1"|"tag:ol17904457261515106613"|"itemlist:2122556388#2022791609#2022793653#2022796489#2022795273#2022792730"|"item_id:2122559529"|"errno:0"|"errmsg:"|"real_ip:192.2.136.127 ] time[ single_db_conn:0 DataService_TraceLog:2 total:167 mem_get:2 t_redis_getm:2 t_redis_con:0 t_redis_con_redis_wd:0 db_query:7 db_query_vshop_r:7 db_con:3 db_con_wd_manage_r:1 db_con_wd_shop_r:1 t_wdsvr-item_0:33 DataService:2 ds_connect:0 t_getRecommonItems_0:20 db_con_vshop_r:1 t_market_0:20 ]"""
      val line4 = """NOTICE: 2017-06-30 18:08:45.378 192.2.46.3 [Base.php:267 Base::printLog()] traceid[108c0000015cf87968e30a022e3b05ea,0] logid[149881732554085514] url[/wd/item/getRecommonItems/itemOder?traceID=1ot4qnj4jp4xr5o6tt&scheme=https&param=%7B%22item_id%22:%222122559529%22,%22page%22:%220%22,%22limit%22:%226%22,%22seller_id%22:%22963259173%22,%22is_wx%22:%220%22%7D] req[{"getRecommonItems":"","traceID":"1ot4qnj4jp4xr5o6tt","scheme":"https","param":"{"item_id":"2122559529","page":"0","limit":"6","seller_id":"963259173","is_wx":"0"}"}] public[] loginfo[localip:192.2.46.59"|"module:shop"|"remote_ip:"|"remote_port:"|"guid:"|"mem_call_num:1"|"tag:ol17904457261515106613"|"itemlist:2122556388#2022791609#2022793653#2022796489#2022795273#2022792730"|"item_id:2122559529"|"errno:0"|"errmsg:"|"real_ip:192.2.136.127 ] time[ single_db_conn:0 DataService_TraceLog:2 total:167 mem_get:2 t_redis_getm:2 t_redis_con:0 t_redis_con_redis_wd:0 db_query:7 db_query_vshop_r:7 db_con:3 db_con_wd_manage_r:1 db_con_wd_shop_r:1 t_wdsvr-item_0:33 DataService:2 ds_connect:0 t_getRecommonItems_0:20 db_con_vshop_r:1 t_market_0:20 ]"""
      val line5 = """NOTICE: 2017-06-30 18:08:45.378 192.2.46.3 [Base.php:267 Base::printLog()] traceid[108c0000015cf87968e30a022e3b05ea,0] logid[149881732554085514] url[/wd/item/getRecommonItems/itemOder?traceID=1ot4qnj4jp4xr5o6tt&scheme=https&param=%7B%22item_id%22:%222122559529%22,%22page%22:%220%22,%22limit%22:%226%22,%22seller_id%22:%22963259173%22,%22is_wx%22:%220%22%7D] req[{"getRecommonItems":"","traceID":"1ot4qnj4jp4xr5o6tt","scheme":"https","param":"{"item_id":"2122559529","page":"0","limit":"6","seller_id":"963259173","is_wx":"0"}"}] public[] loginfo[localip:192.2.46.59"|"module:shop"|"remote_ip:"|"remote_port:"|"guid:"|"mem_call_num:1"|"tag:ol17904457261515106613"|"itemlist:2122556388#2022791609#2022793653#2022796489#2022795273#2022792730"|"item_id:2122559529"|"errno:0"|"errmsg:"|"real_ip:192.2.136.127 ] time[ single_db_conn:0 DataService_TraceLog:2 total:567 mem_get:2 t_redis_getm:2 t_redis_con:0 t_redis_con_redis_wd:0 db_query:7 db_query_vshop_r:7 db_con:3 db_con_wd_manage_r:1 db_con_wd_shop_r:1 t_wdsvr-item_0:33 DataService:2 ds_connect:0 t_getRecommonItems_0:20 db_con_vshop_r:1 t_market_0:20 ]"""
      val line6 = """NOTICE: 2017-06-30 18:08:45.378 192.2.46.3 [Base.php:267 Base::printLog()] traceid[108c0000015cf87968e30a022e3b05ea,0] logid[149881732554085514] url[/wd/item/getRecommonItems/itemOder?traceID=1ot4qnj4jp4xr5o6tt&scheme=https&param=%7B%22item_id%22:%222122559529%22,%22page%22:%220%22,%22limit%22:%226%22,%22seller_id%22:%22963259173%22,%22is_wx%22:%220%22%7D] req[{"getRecommonItems":"","traceID":"1ot4qnj4jp4xr5o6tt","scheme":"https","param":"{"item_id":"2122559529","page":"0","limit":"6","seller_id":"963259173","is_wx":"0"}"}] public[] loginfo[localip:192.2.46.59"|"module:shop"|"remote_ip:"|"remote_port:"|"guid:"|"mem_call_num:1"|"tag:ol17904457261515106613"|"itemlist:2122556388#2022791609#2022793653#2022796489#2022795273#2022792730"|"item_id:2122559529"|"errno:0"|"errmsg:"|"real_ip:192.2.136.127 ] time[ single_db_conn:0 DataService_TraceLog:2 total:12 mem_get:2 t_redis_getm:2 t_redis_con:0 t_redis_con_redis_wd:0 db_query:7 db_query_vshop_r:7 db_con:3 db_con_wd_manage_r:1 db_con_wd_shop_r:1 t_wdsvr-item_0:33 DataService:2 ds_connect:0 t_getRecommonItems_0:20 db_con_vshop_r:1 t_market_0:20 ]"""
      val lines = scala.Array(line1, line2, line3, line4, line5, line6)

      sparkContext.parallelize(lines)
    } else {

      val lastDayFmt = DateUtil.getlastDayFmt

      val file = if (isTest) s"/data/rawlog/autoCollect/www_ynflmrx9e52e701c527e_vshop/${lastDayFmt}/07/*"
      else s"/data/rawlog/autoCollect/www_ynflmrx9e52e701c527e_vshop/${lastDayFmt}/*/*"

      sparkContext.textFile(file)
    }

  }


}