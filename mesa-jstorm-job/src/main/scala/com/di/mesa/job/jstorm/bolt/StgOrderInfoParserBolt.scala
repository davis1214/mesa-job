package com.di.mesa.job.jstorm.bolt

import java.math.BigDecimal
import java.util

import backtype.storm.task.{OutputCollector, TopologyContext}
import backtype.storm.topology.OutputFieldsDeclarer
import backtype.storm.tuple.{Fields, Tuple, Values}
import com.di.mesa.common.util.JsonUtils
import com.di.mesa.plugin.storm.bolt.MesaBaseBolt
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConversions._


/**
  * Created by Davi on 17/8/21.
  */
private[mesa] class StgOrderInfoParserBolt extends MesaBaseBolt {
  private val logger: Logger = LoggerFactory.getLogger(classOf[StgOrderInfoParserBolt])

  private var collector: OutputCollector = null


  override def prepare(stormConf: util.Map[_, _], context: TopologyContext, collector: OutputCollector): Unit = {
    super.prepare(stormConf, context, collector)
    this.collector = collector
    logger.info("task id {} has prepared!", this.taskIndex)
  }

  override def execute(input: Tuple): Unit = {
    beforeExecute

    if (isTickComponent(input)) {
      procTickTuple(input)
      return
    }

    lastTime.set(System.currentTimeMillis)
    val rawLog: String = input.getString(0)
    val parsedMap: util.Map[String, String] = parseRawLog(input, rawLog)
    recordCounter(meticCounter, ParserCost, (System.currentTimeMillis - lastTime.get))

    logger.info("rawlog -> "+ rawLog)


    if (parsedMap.isEmpty) {
      recordCounter(meticCounter, ParserError)
      this.collector.ack(input)
      afterExecute
      return
    }

    // "order_id", "buyer_id", "seller_id", "f_seller_id", "g_seller_id", "add_time", "pay_time", "ship_time", "success_time", "confirm_ship_time",
    // "delay_confirm_ship_time", "update_time", "express_fee", "total_price", "total_fee", "fx_fee", "order_type", "order_source", "order_flags", "order_status", "extend_info"

    this.collector.emit(new Values(parsedMap.get("order_id"), parsedMap.get("buyer_id"), parsedMap.get("seller_id"), parsedMap.get("f_seller_id"), parsedMap.get("g_seller_id"),
      parsedMap.get("add_time"), parsedMap.get("pay_time"), parsedMap.get("ship_time"), parsedMap.get("success_time"), parsedMap.get("confirm_ship_time"),
      parsedMap.get("delay_confirm_ship_time"), parsedMap.get("update_time"), parsedMap.get("express_fee"), parsedMap.get("total_price"), parsedMap.get("total_fee"),
      parsedMap.get("fx_fee"), parsedMap.get("order_type"), parsedMap.get("order_source"), parsedMap.get("order_flags"), parsedMap.get("order_status"), parsedMap.get(
        "extend_info")))

    this.collector.ack(input)
    afterExecute
  }

  private def parseRawLog(input: Tuple, rawLog: String): util.Map[String, String] = {
    var parsedmap: util.Map[String, String] = new util.HashMap[String, String]()

    try {
      val orderMapInfo: util.Map[String, _] = JsonUtils.toMap(rawLog)
      val basicOrderInfo: Any = orderMapInfo.get("orderInfoDO")
      val orderInfoDO: util.Map[String, _] = basicOrderInfo.asInstanceOf[util.Map[String, _]]

      //convert all to string
      for ((key: String, value: Any) <- orderInfoDO) parsedmap.put(key, getStringValue(value, true))
    } catch {
      case e: Exception => {
        logger.error(e.getMessage, e)
      }
      case _ =>
    }
    return parsedmap
  }

  override protected def getStringValue(`object`: AnyRef): String = {
    var value: String = ""
    if (`object` == null) {
      return value
    }
    if (`object`.isInstanceOf[Double]) {
      val d1: BigDecimal = new BigDecimal(`object`.toString)
      if (d1.longValue < 90000) {
        value = String.valueOf(d1.doubleValue)
      }
      else {
        value = String.valueOf(d1.longValue)
      }
    }
    else if (`object`.isInstanceOf[String]) {
      value = `object`.toString
    }
    return value
  }

  protected def getStringValue(`object`: Any, shouldConverToLongFmt: Boolean): String = {
    var value: String = ""
    if (`object` == null) {
      return value
    }
    if (`object`.isInstanceOf[Double]) {
      val d1: BigDecimal = new BigDecimal(`object`.toString)
      if (!shouldConverToLongFmt) {
        value = String.valueOf(d1.doubleValue)
      }
      else {
        value = String.valueOf(d1.longValue)
      }
    }
    else if (`object`.isInstanceOf[String]) {
      value = `object`.toString
    }
    return value
  }

  override def procTickTuple(input: Tuple): Unit = {
    collector.ack(input)
    super.procTickTuple(input)
  }


  override def declareOutputFields(declarer: OutputFieldsDeclarer): Unit = {
    // declarer.declare(new Fields("userId", "time"))
    declarer.declare(new Fields("order_id", "buyer_id", "seller_id", "f_seller_id", "g_seller_id", "add_time", "pay_time", "ship_time", "success_time", "confirm_ship_time",
      "delay_confirm_ship_time", "update_time", "express_fee", "total_price", "total_fee", "fx_fee", "order_type", "order_source", "order_flags", "order_status", "extend_info"))
  }


}
