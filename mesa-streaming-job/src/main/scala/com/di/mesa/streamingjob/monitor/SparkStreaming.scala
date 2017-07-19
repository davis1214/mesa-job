package com.di.mesa.streamingjob.monitor

import com.di.mesa.streamingjob.monitor.manager.KafkaManager
import org.apache.spark.{Logging, SparkConf}

/**
  * Created by davihe on 2016/3/1.
  */
object SparkStreaming extends Logging {

  def main(args: Array[String]) {

    // val Array(topics) = args
    val topics = "mclick"
    val groupId = "test1"

    val zkQuorum = "127.0.0.1:2181/kafka-0.8.1"
    val brokerList = "127.0.0.1:8092"

    val sc = new SparkConf().setAppName("hj-streaming-" + topics).setMaster("local")

    var maxRate = "1000"
    if (args.length > 1) {
      maxRate = args(1)
      logInfo("-------------------------- spark.streaming.kafka.maxRatePerPartition reset to :" + maxRate)
    }
    sc.set("spark.streaming.kafka.maxRatePerPartition", maxRate) //每秒钟最大消费

    var duration = 10

    if (topics.contains("click")) duration = 30 //click日志量小

    val ssc = new StreamingContext(sc, Seconds(duration))

    //ssc.checkpoint("hdfs://heracles/user/bd-warehouse/mydir/test/in")

    val kafkaParam = Map[String, String](
      "zookeeper.connect" -> zkQuorum,
      "metadata.broker.list" -> brokerList,
      "group.id" -> groupId,
      "auto.offset.reset" -> kafka.api.OffsetRequest.LargestTimeString,
      "client.id" -> groupId,
      "zookeeper.connection.timeout.ms" -> "10000"
    )

    val topicsSet = topics.split(",").toSet
    val kc = new KafkaManager(kafkaParam)
    val lines = kc.createDirectStream[String, String, kafka.serializer.StringDecoder, kafka.serializer.StringDecoder](ssc, topicsSet)

    lines.foreachRDD(rdd => {
      if (!rdd.isEmpty()) {
        topics match {
          case "tvadmclick" => //DemoTopic.process(rdd)
          case _ => println("invalid topic")
        }
        kc.updateZKOffsets(rdd)
      }
    })

    ssc.start()

    ssc.awaitTermination()
    //ssc.awaitTerminationOrTimeout(3600000)
    //ssc.awaitTerminationOrTimeout(1800000)
  }
}
