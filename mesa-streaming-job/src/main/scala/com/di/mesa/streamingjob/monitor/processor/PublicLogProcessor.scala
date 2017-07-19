package com.di.mesa.streamingjob.monitor.processor

import com.di.mesa.streamingjob.monitor.common.DatabaseException
import com.di.mesa.streamingjob.monitor.common.RuleEnums
import com.di.mesa.streamingjob.monitor.common.RuleEnums._
import org.apache.spark.rdd.RDD
import org.apache.spark.{ Accumulator, Logging }
import com.di.mesa.streamingjob.monitor.common.CalcTypeEnums
import com.di.mesa.streamingjob.monitor.common.CalcTypeEnums._
import com.di.mesa.streamingjob.monitor.storage.RtStorage

/**
 * Created by davihe on 2016/6/4.
 */
object PublicLogProcessor extends Logging {

  case class Idvisitor(key: String, value: Long)
  case class LineIdvisitor(key: String, value: String)

  //TODO 存储到制定的地方
  //TODO broadcast方式传递filter
  //TODO storageType:String , filter:String
  def process(filterList: List[Map[String, String]], rdd: RDD[(String, String)], lineAccumulator: Accumulator[Long]): Unit = {

    log.info(s"process rdd ${rdd.id}")
    filterList.foreach(filterMap => {
      log.info(filterMap.toString())

      val notNullFilter = { a: String => !a.isEmpty }
      val filterRdd = rdd.map {
        case (k, v) => {
          v
        }
      }.filter(line => {
        logFilter(filterMap, line)
      })

      //filterRdd.collect().foreach { x => println("log filter -->") + x }
      //println(System.currentTimeMillis() + " , filterRdd.count():" + filterRdd.count() + " , rdd.count:" + rdd.count())

      val processedRdd = filterRdd.map(log => {
        lineAccumulator += 1
        logProcess(filterMap, log) //calculate
      })

      //println(System.currentTimeMillis() + " , lineAccumulator : " + lineAccumulator.value + " , " + processedRdd.count())
      //processedRdd.collect().foreach { x => println("log process -->") + x._1.toString() + " , " + x._2 }

      //TODO add state-update
      //kvRdd[DStream].updateStateByKey[Int](updateFunc)
      val tupleRdd = processedRdd.map(idvisitor => (idvisitor._1, idvisitor._2.toLong))

      // processedRdd.collect().foreach(x => println("result:" + x))

      //TODO 考虑实现方式
      def logCaculate(operType: CalcTypeEnums, rdd: RDD[(String, Long)]): RDD[(String, Long)] = {
        //rdd.collect()
        //println("opertype:" + getValueByCalcTypeEnumsIns(operType))
        operType match {
          case DISTINCT => rdd.distinct()
          case AVG      => rdd.groupByKey().map { x => (x._1, x._2.reduce(_ + _) / x._2.count(x => true)) }
          case TOPN     => rdd.map(t => (t._2, t._1)).sortByKey(false).map(t => (t._2, t._1))
          case _        => rdd.reduceByKey(_ + _) //COUNT\SUM
        }
      }

      val operType = filterMap.get(RuleEnums(RULE_MEASURE_TYPE.id).toString()).get
      val logCountRdd = logCaculate(CalcTypeEnums.withName(operType), tupleRdd)

      try {
        val topic = filterMap.get(getValueByRuleEnumsId(RULE_ITEM.id))
        val module = filterMap.get(getValueByRuleEnumsId(RULE_MODULE.id))
        logCountRdd.foreachPartition(RtStorage.opentsPersist(topic, module))
      } catch {
        case e: Exception => throw new DatabaseException("database error")
        case _            => logError("other exception")
      }
    })
  }

  val updateFunc = (currentValues: Seq[Int], preValue: Option[Int]) => {
    val curr = currentValues.sum
    val pre = preValue.getOrElse(0)
    Some(curr + pre)
  }

  def logFilter(filterMap: Map[String, String], line: String): Boolean = {
    def lineFilter(operType: RuleEnums, filter: String): Boolean = {
      if (filter.isEmpty) true //Default value is true
      else
        operType match {
          case RULE_FILTER_CONTAINS => ruleFilterContains(filter, line)
          case RULE_FILTER_RANGE    => ruleFilterRange(filter, line)
        }
    }

    def getFilter(filter: String) = filterMap.getOrElse(filter, "")

    val contains: Boolean = lineFilter(RULE_FILTER_CONTAINS, getFilter(getValueByRuleEnumsIns(RULE_FILTER_CONTAINS)))
    val range: Boolean = lineFilter(RULE_FILTER_RANGE, getFilter(getValueByRuleEnumsIns(RULE_FILTER_RANGE)))

    contains && range
  }

  def logProcess(filterMap: Map[String, String], line: String) = {
    def getFilter(filter: String) = filterMap.getOrElse(filter, "")

    val keyPattern = ruleKeyPattern(getFilter(getValueByRuleEnumsIns(RULE_KEY_PATTERN)), line)
    val ruleMeasureIndex = getFilter(getValueByRuleEnumsIns(RULE_MEASURE_INDEX))
    val valueIndex = if (ruleMeasureIndex.isEmpty) ("", "1") else ruleMeauserIndexValue(ruleMeasureIndex, line)

    val key = if (valueIndex._1.isEmpty()) keyPattern else keyPattern + "=>" + valueIndex._1
    //println("log process:" + key + "=>" + valueIndex._2)
    //LineIdvisitor(key, valueIndex._2)
    (key, valueIndex._2)
  }

  def ruleFilterRange(str: String, line: String): Boolean = {
    def grepFun(): Boolean = {
      str.split("&&&").foreach(one => {
        if (line.indexOf(one) > -1) {
          return true
        }
      })
      false
    }

    def indexFun(): Boolean = {
      str.split("&&").foreach(one => {
        if (line.indexOf(one) == -1) {
          return false
        }
      })
      true
    }

    val result = if (str.contains("&&")) {
      indexFun()
    } else if (str.contains("&&&")) {
      grepFun()
    } else {
      if (line.indexOf(str) == -1) false else true
    }
    result
  }

  def ruleFilterContains(str: String, line: String): Boolean = {
    def grepFun(): Boolean = {
      str.split("&&&").foreach(one => {
        if (line.indexOf(one) > -1) {
          return true
        }
      })
      false
    }

    def indexFun(): Boolean = {
      str.split("&&").foreach(one => {
        if (line.indexOf(one) == -1) {
          return false
        }
      })
      true
    }

    val result = if (str.contains("&&")) {
      indexFun()
    } else if (str.contains("&&&")) {
      grepFun()
    } else {
      if (line.indexOf(str) == -1) false else true
    }
    result
  }

  /**
   * key extractor
   *
   * @param str keys splited by &&
   * @param line
   * @return
   */
  def ruleKeyPattern(str: String, line: String): String = {
    val resultlist = scala.collection.mutable.ListBuffer.empty[String]
    str.split("&&").foreach(one => {
      val urltmp = one.r findFirstIn line match {
        case Some(one.r(url)) => url
        case None             => ""
      }
      if (urltmp == "") {
        return ""
      } else {
        resultlist += urltmp.toString
      }
    })
    resultlist.toList.mkString("->")
  }

  def ruleMeauserIndexValue(str: String, line: String) = {
    val Array(indexKey, indexValue) = str.split("&&", 2)

    val urltmp = indexValue.r findFirstIn line match {
      case Some(indexKey.r(url)) => url
      case None                  => ""
    }

    (indexKey, urltmp)
  }

}
