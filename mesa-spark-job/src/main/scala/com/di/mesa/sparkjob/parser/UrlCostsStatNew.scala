package com.di.mesa.sparkjob.parser

import com.di.mesa.sparkjob.util.{DateUtil, SparkCommon}
import org.apache.spark.rdd.RDD

import java.sql._
import org.json4s.JsonDSL._
import org.json4s._
import org.json4s.jackson.JsonMethods._

import scala.collection.mutable.ArrayBuffer

/**
  * Created by Administrator on 17/7/3.
  */
object UrlCostsStatNew {


  implicit val jsonFormatter = org.json4s.DefaultFormats

  implicit def string2boolean(arg: String): Boolean = arg.toBoolean

  private val pattern = """NOTICE.*?url\[([^\]^\?]*).*module:([^"]*).*total:(\d{1,}).*""".r

  case class StrObject(url: String, module: String, total: Long)

  case class RequestTimeInfo(mode: String, url: String, requestCount: Long, timeCount: String, addTime: Long, updateTime: Long)


  def main(args: scala.Array[String]) {

    val isLocalMode: Boolean = if (args.length > 0) args(0) else "false"

    val datestr = if (args.length > 1) args(1) else DateUtil.getDayOfMonth - 1

    val sourceRdd: RDD[String] = initSourceRdd(isLocalMode)

    val parsedRdd = sourceRdd.map(line => {
      line match {
        case pattern(url, module, total) => StrObject(url, module, total.toLong)
        case _ =>
      }
    }).filter(f => {
      f.isInstanceOf[StrObject] && !f.asInstanceOf[StrObject].url.startsWith("/wd/weixin/callback/")
    }).map(f => {
      val strObjec: StrObject = f.asInstanceOf[StrObject]

      val span = if (strObjec.total <= 50) "50ms"
      else if (strObjec.total > 50 && strObjec.total <= 100) "100ms"
      else if (strObjec.total > 100 && strObjec.total <= 200) "200ms"
      else if (strObjec.total > 200 && strObjec.total <= 500) "500ms"
      else "1s"

      val key1 = strObjec.module.concat("@@").concat(strObjec.url).concat("@@").concat(span)
      (key1, 1)
    }).reduceByKey(_+_)



    val objectParsedRdd = parsedRdd.map(f => {
      try {
        val key1 = f._1.split("@@")
        val key2 = key1(0).concat("@@").concat(key1(1))
        (key2, (key1(2), f._2))
      } catch {
        case e: Exception => ("null@@null", ("0ms", f._2))
      }
    }).groupBy(f => f._1).map(f => {
      var totalRequestCount: Long = 0l
      var metrics = ArrayBuffer[JsonAST.JValue]()

      f._2.foreach(s => {
        totalRequestCount += s._2._2
        metrics += (s._2._1 -> s._2._2)
      })

      val timeCount = compact(render(metrics))
      try {
        val key2 = f._1.split("@@")
        RequestTimeInfo(key2(0), key2(1), totalRequestCount, timeCount, System.currentTimeMillis(), System.currentTimeMillis())
      } catch {
        case e: Exception => RequestTimeInfo(f._1, f._1, totalRequestCount, timeCount, System.currentTimeMillis(), System.currentTimeMillis())
      }
    })


    def mysqlPersistFunc(iter: Iterator[RequestTimeInfo]): Unit = {
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

      val lastDayFmt = DateUtil.getlastDayFmt

      val tableName = s"requset_time_info"
      val deleteSql = s"delete from $tableName where log_date = '${lastDayFmt}' ;"
      val insertSql = s"insert into $tableName (url, mode, request_count,time_count,add_time ,update_time,log_date) values (?, ?, ?, ?, ?, ?, ?) ;"

      var conn: Connection = null
      val d: Driver = null
      var ps: PreparedStatement = null
      var pstmt: PreparedStatement = null

      try {
        val (url: String, user: String, password: String) = getDBConfig(isLocalMode)
        conn = DriverManager.getConnection(url, user, password)
        ps = conn.prepareStatement(deleteSql)
        ps.execute()

        while (iter.hasNext) {
          val item = iter.next()
          pstmt = conn.prepareStatement(insertSql);
          pstmt.setString(1, item.url)
          pstmt.setString(2, item.mode)
          pstmt.setLong(3, item.requestCount)
          pstmt.setString(4, item.timeCount)
          pstmt.setTimestamp(5, new Timestamp(item.addTime))
          pstmt.setTimestamp(6, new Timestamp(item.updateTime))
          pstmt.setDate(7, new Date(DateUtil.getlastDay.getMillis))
          pstmt.executeUpdate();
        }
      } catch {
        case e: Exception => e.printStackTrace()
      } finally {
        if (ps != null) ps.close()
        if (pstmt != null) pstmt.close()
        if (conn != null) conn.close()
      }
    }

    objectParsedRdd.repartition(1).foreachPartition(mysqlPersistFunc)

  }


  def initSourceRdd(isLocalMode: Boolean): RDD[String] = {

    val sparkContext = if (isLocalMode) SparkCommon.getLocalSparkContext
    else SparkCommon.getClusterSparkContext

    if (isLocalMode) {
      val line1 = """NOTICE: 2017-06-30 18:08:45.378 10.2.46.3 [Base.php:267 Base::printLog()] traceid[108c0000015cf87968e30a022e3b05ea,0] logid[149881732554085514] url[/wd/item/getRecommonItems?traceID=1ot4qnj4jp4xr5o6tt&scheme=https&param=%7B%22item_id%22:%222122559529%22,%22page%22:%220%22,%22limit%22:%226%22,%22seller_id%22:%22963259173%22,%22is_wx%22:%220%22%7D] req[{"getRecommonItems":"","traceID":"1ot4qnj4jp4xr5o6tt","scheme":"https","param":"{"item_id":"2122559529","page":"0","limit":"6","seller_id":"963259173","is_wx":"0"}"}] public[] loginfo[localip:10.2.46.59"|"module:item"|"remote_ip:"|"remote_port:"|"guid:"|"mem_call_num:1"|"tag:ol17904457261515106613"|"itemlist:2122556388#2022791609#2022793653#2022796489#2022795273#2022792730"|"item_id:2122559529"|"errno:0"|"errmsg:"|"real_ip:10.2.136.127 ] time[ single_db_conn:0 DataService_TraceLog:2 total:95 mem_get:2 t_redis_getm:2 t_redis_con:0 t_redis_con_redis_wd:0 db_query:7 db_query_vshop_r:7 db_con:3 db_con_wd_manage_r:1 db_con_wd_shop_r:1 t_wdsvr-item_0:33 DataService:2 ds_connect:0 t_getRecommonItems_0:20 db_con_vshop_r:1 t_market_0:20 ]"""
      val line2 = """NOTICE: 2017-06-30 18:08:45.378 10.2.46.3 [Base.php:267 Base::printLog()] traceid[108c0000015cf87968e30a022e3b05ea,0] logid[149881732554085514] url[/wd/item?traceID=1ot4qnj4jp4xr5o6tt&scheme=https&param=%7B%22item_id%22:%222122559529%22,%22page%22:%220%22,%22limit%22:%226%22,%22seller_id%22:%22963259173%22,%22is_wx%22:%220%22%7D] req[{"getRecommonItems":"","traceID":"1ot4qnj4jp4xr5o6tt","scheme":"https","param":"{"item_id":"2122559529","page":"0","limit":"6","seller_id":"963259173","is_wx":"0"}"}] public[] loginfo[localip:10.2.46.59"|"module:item"|"remote_ip:"|"remote_port:"|"guid:"|"mem_call_num:1"|"tag:ol17904457261515106613"|"itemlist:2122556388#2022791609#2022793653#2022796489#2022795273#2022792730"|"item_id:2122559529"|"errno:0"|"errmsg:"|"real_ip:10.2.136.127 ] time[ single_db_conn:0 DataService_TraceLog:2 total:124 mem_get:2 t_redis_getm:2 t_redis_con:0 t_redis_con_redis_wd:0 db_query:7 db_query_vshop_r:7 db_con:3 db_con_wd_manage_r:1 db_con_wd_shop_r:1 t_wdsvr-item_0:33 DataService:2 ds_connect:0 t_getRecommonItems_0:20 db_con_vshop_r:1 t_market_0:20 ]"""
      val line3 = """NOTICE: 2017-06-30 18:08:45.378 10.2.46.3 [Base.php:267 Base::printLog()] traceid[108c0000015cf87968e30a022e3b05ea,0] logid[149881732554085514] url[/wd/item/getRecommonItems/itemOder?traceID=1ot4qnj4jp4xr5o6tt&scheme=https&param=%7B%22item_id%22:%222122559529%22,%22page%22:%220%22,%22limit%22:%226%22,%22seller_id%22:%22963259173%22,%22is_wx%22:%220%22%7D] req[{"getRecommonItems":"","traceID":"1ot4qnj4jp4xr5o6tt","scheme":"https","param":"{"item_id":"2122559529","page":"0","limit":"6","seller_id":"963259173","is_wx":"0"}"}] public[] loginfo[localip:10.2.46.59"|"module:shop"|"remote_ip:"|"remote_port:"|"guid:"|"mem_call_num:1"|"tag:ol17904457261515106613"|"itemlist:2122556388#2022791609#2022793653#2022796489#2022795273#2022792730"|"item_id:2122559529"|"errno:0"|"errmsg:"|"real_ip:10.2.136.127 ] time[ single_db_conn:0 DataService_TraceLog:2 total:167 mem_get:2 t_redis_getm:2 t_redis_con:0 t_redis_con_redis_wd:0 db_query:7 db_query_vshop_r:7 db_con:3 db_con_wd_manage_r:1 db_con_wd_shop_r:1 t_wdsvr-item_0:33 DataService:2 ds_connect:0 t_getRecommonItems_0:20 db_con_vshop_r:1 t_market_0:20 ]"""
      val line4 = """NOTICE: 2017-06-30 18:08:45.378 10.2.46.3 [Base.php:267 Base::printLog()] traceid[108c0000015cf87968e30a022e3b05ea,0] logid[149881732554085514] url[/wd/item/getRecommonItems/itemOder?traceID=1ot4qnj4jp4xr5o6tt&scheme=https&param=%7B%22item_id%22:%222122559529%22,%22page%22:%220%22,%22limit%22:%226%22,%22seller_id%22:%22963259173%22,%22is_wx%22:%220%22%7D] req[{"getRecommonItems":"","traceID":"1ot4qnj4jp4xr5o6tt","scheme":"https","param":"{"item_id":"2122559529","page":"0","limit":"6","seller_id":"963259173","is_wx":"0"}"}] public[] loginfo[localip:10.2.46.59"|"module:shop"|"remote_ip:"|"remote_port:"|"guid:"|"mem_call_num:1"|"tag:ol17904457261515106613"|"itemlist:2122556388#2022791609#2022793653#2022796489#2022795273#2022792730"|"item_id:2122559529"|"errno:0"|"errmsg:"|"real_ip:10.2.136.127 ] time[ single_db_conn:0 DataService_TraceLog:2 total:167 mem_get:2 t_redis_getm:2 t_redis_con:0 t_redis_con_redis_wd:0 db_query:7 db_query_vshop_r:7 db_con:3 db_con_wd_manage_r:1 db_con_wd_shop_r:1 t_wdsvr-item_0:33 DataService:2 ds_connect:0 t_getRecommonItems_0:20 db_con_vshop_r:1 t_market_0:20 ]"""
      val line5 = """NOTICE: 2017-06-30 18:08:45.378 10.2.46.3 [Base.php:267 Base::printLog()] traceid[108c0000015cf87968e30a022e3b05ea,0] logid[149881732554085514] url[/wd/item/getRecommonItems/itemOder?traceID=1ot4qnj4jp4xr5o6tt&scheme=https&param=%7B%22item_id%22:%222122559529%22,%22page%22:%220%22,%22limit%22:%226%22,%22seller_id%22:%22963259173%22,%22is_wx%22:%220%22%7D] req[{"getRecommonItems":"","traceID":"1ot4qnj4jp4xr5o6tt","scheme":"https","param":"{"item_id":"2122559529","page":"0","limit":"6","seller_id":"963259173","is_wx":"0"}"}] public[] loginfo[localip:10.2.46.59"|"module:shop"|"remote_ip:"|"remote_port:"|"guid:"|"mem_call_num:1"|"tag:ol17904457261515106613"|"itemlist:2122556388#2022791609#2022793653#2022796489#2022795273#2022792730"|"item_id:2122559529"|"errno:0"|"errmsg:"|"real_ip:10.2.136.127 ] time[ single_db_conn:0 DataService_TraceLog:2 total:567 mem_get:2 t_redis_getm:2 t_redis_con:0 t_redis_con_redis_wd:0 db_query:7 db_query_vshop_r:7 db_con:3 db_con_wd_manage_r:1 db_con_wd_shop_r:1 t_wdsvr-item_0:33 DataService:2 ds_connect:0 t_getRecommonItems_0:20 db_con_vshop_r:1 t_market_0:20 ]"""
      val line6 = """NOTICE: 2017-06-30 18:08:45.378 10.2.46.3 [Base.php:267 Base::printLog()] traceid[108c0000015cf87968e30a022e3b05ea,0] logid[149881732554085514] url[/wd/item/getRecommonItems/itemOder?traceID=1ot4qnj4jp4xr5o6tt&scheme=https&param=%7B%22item_id%22:%222122559529%22,%22page%22:%220%22,%22limit%22:%226%22,%22seller_id%22:%22963259173%22,%22is_wx%22:%220%22%7D] req[{"getRecommonItems":"","traceID":"1ot4qnj4jp4xr5o6tt","scheme":"https","param":"{"item_id":"2122559529","page":"0","limit":"6","seller_id":"963259173","is_wx":"0"}"}] public[] loginfo[localip:10.2.46.59"|"module:shop"|"remote_ip:"|"remote_port:"|"guid:"|"mem_call_num:1"|"tag:ol17904457261515106613"|"itemlist:2122556388#2022791609#2022793653#2022796489#2022795273#2022792730"|"item_id:2122559529"|"errno:0"|"errmsg:"|"real_ip:10.2.136.127 ] time[ single_db_conn:0 DataService_TraceLog:2 total:12 mem_get:2 t_redis_getm:2 t_redis_con:0 t_redis_con_redis_wd:0 db_query:7 db_query_vshop_r:7 db_con:3 db_con_wd_manage_r:1 db_con_wd_shop_r:1 t_wdsvr-item_0:33 DataService:2 ds_connect:0 t_getRecommonItems_0:20 db_con_vshop_r:1 t_market_0:20 ]"""
      val lines = scala.Array(line1, line2, line3, line4, line5, line6)

      sparkContext.parallelize(lines)
    } else {

      val lastDayFmt = DateUtil.getlastDayFmt

      val file = s"/data/rawlog/autoCollect/www_ynflmrx9e52e701c527e_vshop/${lastDayFmt}/07/*"

      sparkContext.textFile(file)
    }


  }


}