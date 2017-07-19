package com.di.mesa.streamingjob.monitor.job

import javax.annotation.Nonnull
import com.di.mesa.streamingjob.monitor.common.DatabaseException
import com.di.mesa.streamingjob.monitor.processor.UdcLogProcessor
import org.apache.commons.cli._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.{ Seconds, StreamingContext }
import com.di.mesa.streamingjob.monitor.manager.KafkaManager
import org.apache.spark.{ SparkConf, Logging }
import org.apache.hadoop.hdfs.web.resources.OverwriteParam
import java.util.concurrent.atomic.AtomicBoolean

/**
 * Created by Administrator on 16/5/30.
 *
 *
 * UDC_LOG 在spark-streaming 的实现
 *
 */
object UdcLogJob extends Logging {

  var lst = List(Map("index" -> "NOTICE", "pattern" -> """url\[([^(\?|\])]+).*?\]"""))

  def main(args: Array[String]) = {
    def daemonThread(ssc: org.apache.spark.streaming.StreamingContext) = {
      val thread = new Thread(new Runnable() {
        override def run() {
          val shouldStop = new AtomicBoolean(false)
          while (!shouldStop.get) {
            if (!ssc.sparkContext.getConf.contains("spark.app.id")) {
              Thread.sleep(2000)
              println("appid is not esits")
            } else {
              shouldStop.set(true)
              //TODO 直接更新数据库
              println("application id is :" + ssc.sparkContext.getConf.getAppId)
              //TODO test
              updateAppIdByJobId("id_00001", ssc.sparkContext.getConf.getAppId)
            }
          }
        }
      })
      thread.setDaemon(true)
      thread
    }

    //brokerlist

    try {
      val Array(zkQuorum, brokerlist, topic, groupId /*group id*/ , duration, ruleFile, rate) = initParam(args)
      val sc = new SparkConf().setAppName(UdcLogJob.getClass.getSimpleName + "_Job")
      sc.set("spark.streaming.kafka.maxRatePerPartition", rate) //每秒钟最大消费
      //sc.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")

      //TODO for test
      if ("test".equals(topic)) {
        sc.setMaster("local")
      }

      val ssc = new StreamingContext(sc, Seconds(duration.toLong))
      if (!"test".equals(topic))
        ssc.checkpoint("hdfs://argo/user/www/output/udc_test")

      //TODO 选择数据源,做下封装
      val kafkaParam = Map[String, String](
        "zookeeper.connect" -> zkQuorum,
        "metadata.broker.list" -> brokerlist,
        "group.id" -> groupId,
        "auto.offset.reset" -> kafka.api.OffsetRequest.LargestTimeString,
        "client.id" -> groupId,
        "zookeeper.connection.timeout.ms" -> "10000")

      logInfo("filter params as follows")
      //initStatRule(ruleFile)
      getRtRuleByJobId("id_00001")
      lst.foreach(println)

      val broadcast: Broadcast[List[Map[String, String]]] = ssc.sparkContext.broadcast(lst)
      val topicsSet = topic.split(",").toSet
      val kc = new KafkaManager(kafkaParam)
      val lines = kc.createDirectStream[String, String, kafka.serializer.StringDecoder, kafka.serializer.StringDecoder](ssc, topicsSet)

      //1 过滤条件
      //2 统计条件
      //3 数据输出

      //TODO 执行计算.针对美中类型,分型计算
      try {
        val lbatchAccumulator = ssc.sparkContext.accumulator(1l, "line-batch-accumulator")
        val lineAccumulator = ssc.sparkContext.accumulator(1l, "line-accumulator")

        lbatchAccumulator += 1
        lines.foreachRDD(rdd => {
          if (!rdd.isEmpty()) {
            topic match {
              //TODO 工厂模式
              case "app_udc_account" => UdcLogProcessor.process(broadcast.value, rdd, lineAccumulator)
              case "test"            => UdcLogProcessor.process(broadcast.value, rdd, lineAccumulator)
              case _                 => logError("error topic")
            }

            logInfo("line-batch-accumulator:" + lbatchAccumulator.value + " , line-accumulator:" + lineAccumulator)

            //TODO 拆分开
            kc.updateZKOffsets(rdd)
          } else {
            logInfo("current rdd is empty")
          }
        })
      } catch {
        case e: DatabaseException => logError(e.getMessage, e)
        case _                    => logError("other exceptions")
      }

      ssc.start()
      daemonThread(ssc).start()
      ssc.awaitTermination()
      println("job stopped!")
    } catch {
      case e: Exception =>
      case _            => println("other")
    }

  }

  def initParam(args: Array[String]): Array[String] = {
    // 1、初始化参数
    val options = createOptions();
    val parser: CommandLineParser = new BasicParser();
    val commandLine: CommandLine = parser.parse(options, args);

    if (commandLine.hasOption("h")) {
      showHelp(options);
    }

    val zkQuorum: String = commandLine.getOptionValue("zks", "localhost:2181")
    val brokerlist: String = commandLine.getOptionValue("brokers", "localhost:9092")
    val topic: String = commandLine.getOptionValue("topic", "test");
    val groupid: String = commandLine.getOptionValue("groupid", "test");


    val duration: String = commandLine.getOptionValue("duration", "20")
    val ruleFile: String = commandLine.getOptionValue("rule", "/Users/Administrator/Documents/test/udc_log.txt")
    val rate: String = commandLine.getOptionValue("rate", "1000")

    if (zkQuorum.isEmpty() || brokerlist.isEmpty() || topic.isEmpty()) {
      logError("You must specify parameters!\n");
      showHelp(options);
    }

    Array(zkQuorum, brokerlist, topic, groupid /*group id*/ , duration, ruleFile, rate)

  }

   import java.sql._
  import com.di.mesa.streamingjob.monitor.util.ConnectionPool

 // case class RtJobBean(ruleItem: String, ruleModule: String, ruleFilterFontains: String, ruleFilterRange: String, ruleKeyPattern: String, ruleMeasureType: String, ruleMeasureIndex: String)

  def getRtRuleByJobId(jobId: String): Unit = {
    Class.forName("com.mysql.jdbc.Driver")
    val rulesBuffer = scala.collection.mutable.ArrayBuffer[Map[String, String]]()
    val conn_str = "jdbc:mysql://localhost:3306/test?user=root&password=root"
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
        if (ruleItem != null) map += ("RULE_ITEM" -> ruleItem)
        if (ruleModule != null) map += ("RULE_MODULE" -> ruleModule)
        if (ruleFilterFontains != null) map += ("RULE_FILTER_CONTAINS" -> ruleFilterFontains)
        if (ruleFilterRange != null) map += ("RULE_FILTER_RANGE" -> ruleFilterRange)
        if (ruleKeyPattern != null) map += ("RULE_KEY_PATTERN" -> ruleKeyPattern)
        if (ruleMeasureType != null) map += ("RULE_MEASURE_TYPE" -> ruleMeasureType)
        if (ruleMeasureIndex != null) map += ("RULE_MEASURE_INDEX" -> ruleMeasureIndex)

        rulesBuffer += map.toMap
      }
    } finally {
      conn.close()
    }
    lst = rulesBuffer.toList
  }

  def updateAppIdByJobId(jobId: String, applicationId: String): Unit = {
    val sql = "UPDATE RT_JOB SET Application_Id = ? WHERE ID = ?"
    val conn = ConnectionPool.getConnection.getOrElse(null)
    val ps = conn.prepareStatement(sql)
    ps.setString(1, applicationId)
    ps.setString(2, jobId)

    ps.executeUpdate()
    ps.close()
    ConnectionPool.closeConnection(conn)
  }
  
  def initStatRule(ruleFile: String) {
    val file = "/Users/Administrator/Documents/test/udc_log.txt"
    val fileName = if (ruleFile.isEmpty) file else file
    lst = scala.io.Source.fromFile(fileName).getLines.map { log => log.split("\\001").map { x => x.split("\\002") }.filter(_.length == 2).map { x => (x(0), x(1)) }.toMap }.toList
  }

  def createOptions(): Options = {
    val options = new Options()
    options.addOption("zks", "zkquorum", true, "zookeeper Quorum");
    options.addOption("brokers", "brokelist", true, "broker lists");
    options.addOption("duration", "duration", true, "windows size of spark streaming");
    options.addOption("topic", "topic", true, "topic");
    options.addOption("groupid", "groupid", true, "groupid");
    options.addOption("rule", "rule", false, "rule file used on processing");
    options.addOption("rate", "rate", false, "maxRate of per-partition to fetch message from kafka");
    options.addOption("h", "help", false, "Help");
    options
  }

  def showHelp(@Nonnull options: Options): Unit = {
    val helpFormatter = new HelpFormatter();
    helpFormatter.printHelp("help", options);
    System.exit(-1);
  }

}
