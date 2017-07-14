package com.di.mesa.job.jstorm.bolt

import java.util

import backtype.storm.task.{OutputCollector, TopologyContext}
import backtype.storm.topology.OutputFieldsDeclarer
import backtype.storm.topology.base.BaseRichBolt
import backtype.storm.tuple.{Values, Fields, Tuple}
import com.di.mesa.job.jstorm.utils.TupleHelpers
import jregex.{Matcher, Pattern}
import org.apache.log4j.Logger

import scala.util.matching._


/**
  * Created by Administrator on 17/6/28.
  */
class IMParserBolt(AppTopic: String) extends BaseRichBolt {

  @transient lazy private val LOG: Logger = Logger.getLogger(classOf[IMParserBolt])


  var collector: OutputCollector = _

  //TODO 正则还有些问题
  var regex: Regex = _

  val appFilter: String = "appstatus:background"

  val pcFilter = Array("https://gwh5.api.mesa.com/wd/seller/info/getPCMixedData", "https://gwh5.api.mesa.com/wd/shop/base/getShopFlagInfo", "https://gwh5.api.mesa.com/wd/notice/getNoticeList", "https://gwh5.api.mesa.com/wd/order/shop/getAuthority", "https://gwh5.api.mesa.com/wd/order/seller/getOrderNumAuth", "https://gwh5.api.mesa.com/wd/order/order/getPCListAuth", "https://gwh5.api.mesa.com/wd/order/seller/getCompleteTaskCnt", "https://gwh5.api.mesa.com/wd/item/getItemsListAuth", "https://gwh5.api.mesa.com/wd/item/updateItemDetail")

  val logPattern =
    """"requestTime:({requestTime}\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d).*?requesturl:({requestUrl}.*?)".*userID:({userID}\d{1,})"""

  var pattern: Pattern = _


  case class UserInfo(requestTime: String, requestUrl: String, userId: String)

  override def prepare(stormConf: util.Map[_, _], context: TopologyContext, collector: OutputCollector): Unit = {
    this.collector = collector;

    pattern = new Pattern(logPattern)
  }

  override def execute(tuple: Tuple): Unit = {
    this.collector.ack(tuple)

    if (TupleHelpers.isTickTuple(tuple)) {
      LOG.info(s"wow ,it's tick time ${Thread.currentThread().getName}")
      return
    }

    val log = tuple.getString(0)

    // val regex(time, userId) = """ss"""


    if (tuple.getSourceComponent().equals("www_proxy_gw_app_audittrail")) {
      if (log.contains(appFilter)) {
        return
      }
    }


    //区分不同的topic,根据然后增加过滤条件
    val matcher: Matcher = pattern.matcher(log)
    if (matcher.find()) {
      val userInfo = UserInfo(matcher.group("requestTime"), matcher.group("requestUrl"), matcher.group("userID"))
      if (!pcFilter.contains(userInfo.requestUrl)) {
        return
      }
      this.collector.emit(new Values(userInfo.userId, userInfo.requestTime))
    }

  }


  override def declareOutputFields(declarer: OutputFieldsDeclarer): Unit = {
    declarer.declare(new Fields("userId", "time"))
  }
}
