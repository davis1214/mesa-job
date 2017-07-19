package com.di.mesa.streamingjob.monitor.rts

import javax.annotation.Nonnull

import com.di.mesa.streamingjob.monitor.processor.UdcLogProcessor
import kafka.tools.MirrorMaker._
import org.apache.commons.cli.{BasicParser, CommandLine, CommandLineParser, HelpFormatter, Options}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import com.di.mesa.streamingjob.monitor.manager.KafkaManager
import org.apache.spark.{Logging, SparkConf};


/**
  * Created by davihe on 2016/3/1.
  */
object SparkStreaming extends Logging {

  def main(args: Array[String]) {

    try {
      //TODO 参数及初始化,需要根据不同的类型,选择不同的校验.目前只实现kafka
      val Array(zkQuorum,brokerlist,topic,groupId/*group id*/,duration) = initParam(args)

      val sc = new SparkConf().setMaster("local").setAppName("test")

      sc.set("spark.streaming.kafka.maxRatePerPartition", "1000") //每秒钟最大消费

      val ssc = new StreamingContext(sc, Seconds(duration.toLong))

      //ssc.checkpoint("hdfs://heracles/user/data/test/in")


      //TODO 选择数据源,做下封装
      val kafkaParam = Map[String, String](
        "zookeeper.connect" -> zkQuorum,
        "metadata.broker.list" -> brokerlist,
        "group.id" -> groupId,
        "auto.offset.reset" -> kafka.api.OffsetRequest.LargestTimeString,
        "client.id" -> groupId,
        "zookeeper.connection.timeout.ms" -> "10000"
      )

      val topicsSet = topic.split(",").toSet
      val kc = new KafkaManager(kafkaParam)
      val lines = kc.createDirectStream[String, String, kafka.serializer.StringDecoder, kafka.serializer.StringDecoder](ssc, topicsSet)


      //TODO 执行计算.针对美中类型,分型计算
      lines.foreachRDD(rdd => {
        if (!rdd.isEmpty()) {
          topic match {

            //case "test" => DemoTopic.process(rdd)
            case _ => println("invalid topic")
          }

          //TODO 拆分开
          kc.updateZKOffsets(rdd)
        }
      })

      //TODO 获取页面的applicationID

      ssc.start()

      //TODO 获取页面的applicationID

      ssc.start()

      //println("application Id  : " + sc.getAppId)


      Thread.sleep(20000)

      ssc.stop(true,true)

      ssc.awaitTermination()


      println("job stopped!")
      //得到当前的applicationId,然后统一去管理.  yarn application kill appid
      //println("application Id  : " + sc.getAppId)


      Thread.sleep(20000)

      ssc.stop(true,true)

      ssc.awaitTermination()


      println("job stopped!")
      //得到当前的applicationId,然后统一去管理.  yarn application kill appid



    } catch {
      case e: Exception => e.printStackTrace()
      //case _ => println("other")
    }


  }

  def initParam(args: Array[String]) :Array[String] ={
    // 1、初始化参数
    val options = createOptions();
    val parser: CommandLineParser = new BasicParser();
    val commandLine: CommandLine = parser.parse(options, args);

    if (commandLine.hasOption("h")) {
      showHelp(options);
    }

    val zkQuorum: String = commandLine.getOptionValue("zks","localhost:2181")
    val brokerlist: String = commandLine.getOptionValue("brokers","localhost:9092")
    val topic: String = commandLine.getOptionValue("topic","test");

    /*val zkQuorum: String = commandLine.getOptionValue("zks","")
    val brokerlist: String = commandLine.getOptionValue("brokers","")
    val topic: String = commandLine.getOptionValue("topic","");
    val duration:String = commandLine.getOptionValue("duration","0")*/
    //val groupId = topic

    val duration = "100"

    if (zkQuorum.isEmpty() || brokerlist.isEmpty() || topic.isEmpty()) {
      println("You must specify parameters!\n");
      showHelp(options);
    }

    Array(zkQuorum,brokerlist,topic,topic/*group id*/,duration)

  }

  def createOptions(): Options = {
    val options = new Options()
    options.addOption("zks", "zkquorum", true, "zookeeper Quorum");
    options.addOption("brokers", "brokelist", true, "broker lists");
    options.addOption("duration", "duration", true, "windows size of spark streaming");
    options.addOption("h", "help", false, "Help");
    options
  }

  def showHelp(@Nonnull options: Options): Unit = {
    val helpFormatter = new HelpFormatter();
    helpFormatter.printHelp("help", options);
    System.exit(-1);
  }

}
