package com.di.mesa.job.jstorm.utils

import java.util.Date;
import java.text.SimpleDateFormat;
import scala.collection.mutable.HashMap;
import java.text._;

class LogProxyMeta(val req: List[String]) {

  val TOKEN_SPLIT = """"\|""""
  val MAP_SPLIT = """:"""
  val PARAM_SPLIT = ""","""
  var reqField: List[String] = req

  def line2map(line: String, paramSet: Set[String]): Map[String, String] = {
    if (line == null || line.trim.length <= 1) {
      val resMap: Map[String, String] = Map()
      resMap
    } else {
      val tmpMap = splitLine(line)
      val fieldsMap = tmpMap.filterKeys(paramSet)
      fieldsMap
    }

  }

  def strtotime(strDate: String): String = {
    val sd = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    try {
      val d = sd.parse(strDate);
      val timestamp = d.getTime() / 1000;
      //得到秒数，Date类型的getTime()返回毫秒数
      val strtime = timestamp.toString()
      strtime
    } catch {
      case e: ParseException => ""
    }
  }

  def formateUrl(requesturl: String): String = {

    val urlPattern = ".*(?i)(taoke|getItemInfo|addProduct).*" r
    val isMatch = urlPattern.pattern.matcher(requesturl).matches
    if (isMatch) {
      val urlPattern(urlType) = requesturl

      urlType match {
        case "taoke"       => "jump"
        case "getItemInfo" => "item"
        case "addProduct"  => "favorite"
        case _             => "UNKNOW"
      }

    } else {
      "other"
    }

  }

  def initFieldKey(fields: String): Set[String] = {
    val tmpSet = fields.split(PARAM_SPLIT).toSet
    tmpSet
  }

  def splitLine(line: String): Map[String, String] = {
    val length = line.length
    val lineTrim = line.substring(1, length - 1)
    val aumap = lineTrim.split(TOKEN_SPLIT).toList.map(x => x.split(MAP_SPLIT, 2)).map(x => {
      x.length match {
        case 2 => Tuple2(x(0), x(1))
        case 1 => Tuple2(x(0), "")
        case _ => Tuple2("", "")
      }
    }).toMap
    aumap
  }

  def getReqFields(line: String): Map[String, String] = {
    val allFields = splitLine(line)

    val resFields = new HashMap[String, String]()
    for (field <- reqField) {

      resFields.put(field, allFields.getOrElse(field, ""))

    }
    resFields.toMap
  }

  def getAllFields(line: String): Map[String, String] = {
    val allFields = splitLine(line)
    allFields
  }

}
