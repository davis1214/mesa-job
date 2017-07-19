package com.di.mesa.streamingjob.monitor.job

import org.apache.commons.cli.BasicParser
import org.apache.commons.cli.CommandLine
import org.apache.commons.cli.CommandLineParser
import org.apache.commons.cli.HelpFormatter
import org.apache.commons.cli.Options
import org.apache.spark.Logging

import javax.annotation.Nonnull
import com.di.mesa.streamingjob.monitor.common.RuleEnums._
import scopt.OptionParser

trait RtJob extends Logging {

  //var filterList = List(Map("index" -> "NOTICE", "pattern" -> """url\[([^(\?|\])]+).*?\]"""))
  //var lst: List[Map[String, String]] = List(Map())

  var lst = List(Map("Rule_Module" -> "topic_udc_log_app", "Rule_Item" -> "app_udc_account", "Rule_Filter_Contains" -> "NOTICE", "Rule_Key_Pattern" -> """url\[([^(\?|\])]+).*?\]""", "Rule_Measure_Type" -> "SUM"))

  case class JobParam(zkQuorum: String = "localhost:2181", brokerlist: String = "localhost:9092", topic: String = "test1", groupId: String = "test1_groupid", duration: Long = 30, rate: String = "1000", jobId: String = "jobid0001")
  val defaultParams = JobParam()

  val parser = new OptionParser[JobParam]("PublicJob Filtering") {
    head("Basic Job Param Filter")
    opt[String]("topic")
      .required()
      .text(s"topic")
      .action((x, c) => c.copy(topic = x))
    opt[String]("zks")
      .required()
      .text(s"zookeeper Quorum")
      .action((x, c) => c.copy(zkQuorum = x))
    opt[String]("brokers")
      .required()
      .text(s"broker lists")
      .action((x, c) => c.copy(brokerlist = x))
    opt[String]("jobid")
      .required()
      .text(s"job id")
      .action((x, c) => c.copy(jobId = x))
    opt[Long]("duration")
      .text(s"windows size of spark streaming,default 30s")
      .action((x, c) => c.copy(duration = x))
    opt[String]("groupid")
      .text(s"groupid,default is topic name")
      .action((x, c) => c.copy(groupId = x))
    opt[String]("rate")
      .text(s"maxRate of per-partition to fetch message from kafka, default 1000")
      .action((x, c) => c.copy(rate = x))

    note(
      """
          |Usage:
          | -topic,<arg>         topic
          | -zks,<arg>           zookeeper Quorum
          | -brokers,<arg>       broker lists
          | -duration, <arg>     windows size of spark streaming
          | -groupid, <arg>      groupid
          | -jobid ,<arg>        jobId
          | -rate,<arg>          maxRate of per-partition to fetch message from kafka, default 1000
          | 
          | 
          | /usr/local/webserver/spark-1.5.1-bin-2.6.0/bin/spark-submit 
          |   --conf spark.serializer=org.apache.spark.serializer.KryoSerializer 
          |   --class com.di.mesa.streamingjob.monitor.job.PublicRtJob
          |   --master yarn-client --num-executors 6 --queue spark --total-executor-cores 3 --driver-memory 2G 
          |   --jars /home/www/jj/kafka_2.10-0.8.2.1.jar,/home/www/jj/kafka-clients-0.8.2.1.jar,/home/www/jj/mysql-connector-java-5.1.12.jar,/home/www/jj/bonecp-0.8.0.RELEASE.jar,/usr/local/webserver/spark-1.5.1-bin-2.6.1/lib/spark-examples-1.5.1-hadoop2.6.0.jar /home/www/jj/rt-job-1.0-SNAPSHOT.jar -zks localhost:2181
          |   --duration 30 
          |   --brokers 127.0.0.1:9092
          |   --topic app_udc_account 
          |   --groupid app__groupid0001
          |   --jobid jobid0001
        """.stripMargin)
  }

  def buildKakfaParam(param: JobParam) = {
    val kafkaParam = Map[String, String](
      "zookeeper.connect" -> param.zkQuorum,
      "metadata.broker.list" -> param.brokerlist,
      "group.id" -> param.groupId,
      "auto.offset.reset" -> kafka.api.OffsetRequest.LargestTimeString,
      "client.id" -> param.groupId,
      "zookeeper.connection.timeout.ms" -> "10000")
    kafkaParam
  }

  def initStatRule(ruleFile: String) {
    val file = "/Users/Administrator/Documents/test/udc_log.txt"
    val fileName = if (ruleFile.isEmpty) file else file
    lst = scala.io.Source.fromFile(fileName).getLines.map { log => log.split("\\001").map { x => x.split("\\002") }.filter(_.length == 2).map { x => (x(0), x(1)) }.toMap }.toList

  }

  import java.sql._
  import com.di.mesa.streamingjob.monitor.util.ConnectionPool

  val conn_str = "jdbc:mysql://localhost:3306/test?user=root&password=root"

  def initFilters(jobId: String): List[Map[String, String]] = {
    val rulesBuffer = scala.collection.mutable.ArrayBuffer[Map[String, String]]()

    Class.forName("com.mysql.jdbc.Driver")
    val conn = DriverManager.getConnection(conn_str)

    try {
      val sql = "SELECT rule_item,rule_module,rule_filter_contains,rule_filter_range,rule_key_pattern,rule_measure_type,rule_measure_index FROM RT_RULE WHERE is_valid = '1' AND RT_JOB_ID = ? "
      val statement = conn.prepareStatement(sql)
      statement.setString(1, jobId)
      val rs = statement.executeQuery()

      while (rs.next()) {
        val scala.Array(ruleItem, ruleModule, ruleFilterFontains, ruleFilterRange, ruleKeyPattern, ruleMeasureType, ruleMeasureIndex) = scala.Array(rs.getString("rule_item"), rs.getString("rule_module"), rs.getString("rule_filter_contains"),
          rs.getString("rule_filter_range"), rs.getString("rule_key_pattern"), rs.getString("rule_measure_type"),
          rs.getString("rule_measure_index"))

        //var lst = List(Map("Rule_Filter_Contains" -> "NOTICE", "Rule_Key_Pattern" -> """url\[([^(\?|\])]+).*?\]""", "Rule_Measure_Type" -> "SUM"))

        val map = new scala.collection.mutable.LinkedHashMap[String, String]
        if (ruleItem != null) map += (getValueByRuleEnumsId(RULE_ITEM.id) -> ruleItem)
        if (ruleModule != null) map += (getValueByRuleEnumsId(RULE_MODULE.id) -> ruleModule)
        if (ruleFilterFontains != null) map += (getValueByRuleEnumsId(RULE_FILTER_CONTAINS.id) -> ruleFilterFontains)
        if (ruleFilterRange != null) map += (getValueByRuleEnumsId(RULE_FILTER_RANGE.id) -> ruleFilterRange)
        if (ruleKeyPattern != null) map += (getValueByRuleEnumsId(RULE_KEY_PATTERN.id) -> ruleKeyPattern)
        if (ruleMeasureType != null) map += (getValueByRuleEnumsId(RULE_MEASURE_TYPE.id) -> ruleMeasureType)
        if (ruleMeasureIndex != null) map += (getValueByRuleEnumsId(RULE_MEASURE_INDEX.id) -> ruleMeasureIndex)

        rulesBuffer += map.toMap
      }
    } finally {
      conn.close()
    }

    log.info(s"filters ${rulesBuffer.toList}")
    rulesBuffer.toList
    //lst
  }

  def updateAppIdByJobId(jobId: String, applicationId: String): Unit = {
    try {
      val sql = "UPDATE RT_JOB SET Application_Id = ?,Update_Time =? WHERE ID = ?"
      //val conn = ConnectionPool.getConnection.getOrElse(null)
      val conn = DriverManager.getConnection(conn_str)
      val ps = conn.prepareStatement(sql)
      ps.setString(1, applicationId)
      ps.setTimestamp(2, new java.sql.Timestamp(System.currentTimeMillis()));
      ps.setString(3, jobId)

      ps.executeUpdate()
      ps.close()
      conn.close()
      log.info(s"UPDATE RT_JOB with param jobId:${jobId} , applicationId:${applicationId}")
    } catch {
      case e: Exception => e.printStackTrace() // TODO: handle error
    }
  }

}