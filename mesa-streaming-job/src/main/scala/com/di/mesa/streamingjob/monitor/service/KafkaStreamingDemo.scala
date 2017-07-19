package com.di.mesa.streamingjob.monitor.service

import com.di.mesa.streamingjob.monitor.SparkStreaming
import SparkStreaming._
import com.di.mesa.streamingjob.monitor.SparkStreaming
import com.di.mesa.streamingjob.monitor.util.SplitUtil
import com.di.mesa.streamingjob.monitor.manager.KafkaManager
import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by davihe on 2016/4/21.
  */
object KafkaStreamingDemo {

  def main(args: Array[String]) {

    val updateFunc = (values: Seq[Int], state: Option[Int]) => {
      val currentCount = values.sum
      val previousCount = state.getOrElse(0)
      Some(currentCount + previousCount)
    }

    val ordinary = (('a' to 'z') ++ ('A' to 'Z') ++ ('0' to '9')).toSet

    val newUpdateFunc = (iterator: Iterator[(String, Seq[Int], Option[Int])]) => {
      iterator.flatMap(t => updateFunc(t._2, t._3).map(s => (t._1, s)))
    }

    def splitBusiFunc(line: String): (String, Int) = {
      try {
        val logs = line.split(SplitUtil.SPLIT)
        val timeStr = SplitUtil.parseLogDate(logs(1), "yyyy-MM-dd-HH")
        val logParam = SplitUtil.parseUrlParam(logs(4))
        val bookpackageId = logParam.get("b")
        val platId = logParam.get("plat")
        if (bookpackageId != None && platId != None) {
          //(bookpackageId.get + SplitUtil.ROW_KEY_SPLIT + timeStr + SplitUtil.ROW_KEY_SPLIT + platId.get, 1)
          (timeStr, 1)
        } else {
          ("err", 0)
        }
      } catch {
        case e: Exception => ("err", 0)
      }
    }

    val Array(topics, groupId) = Array("tvadmclick", "test")
    val zkQuorum = "10.16.10.76:2181,10.16.10.94:2181,10.16.10.99:2181,10.16.34.187:2181,10.16.34.166:2181,10.16.34.159:2181,10.16.34.58:2181/kafka-0.8.1"
    val brokerList = "10.16.10.196:8092,10.16.10.197:8092,10.16.10.198:8092,10.16.10.199:8092,10.16.10.200:8092,10.16.10.163:8092,10.16.10.164:8092,10.16.10.165:8092,10.16.10.172:8092,10.16.43.148:8092,10.16.43.149:8092,10.16.43.150:8092,10.16.43.151:8092,10.16.43.152:8092,10.16.43.156:8092,10.16.43.153:8092,10.16.43.154:8092,10.16.43.155:8092"

    val sc = new SparkConf().setAppName("test-streaming-" + topics).setMaster("local")

    var maxRate = "1000"
    if (args.length > 1) {
      maxRate = args(1)
      println("-------------------------- spark.streaming.kafka.maxRatePerPartition reset to :" + maxRate)
    }
    sc.set("spark.streaming.kafka.maxRatePerPartition", maxRate) //每秒钟最大消费

    var duration = 10
    if (topics.contains("click")) duration = 20 //click日志量小

    val ssc = new StreamingContext(sc, Seconds(duration))
    ssc.checkpoint("hdfs://10.10.52.98:9000/user/data/checkpoint/spark-streaming/tvadmclick")

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
    val messages = kc.createDirectStream[String, String, kafka.serializer.StringDecoder, kafka.serializer.StringDecoder](ssc, topicsSet)

    //TODO 需要更新offset值，参考tvadmclick.scala

    val lines = messages.map(_._2).filter(a => {
      !a.contains("&vp=e") && a.contains("&b=")
    })
    val wordDstream = lines.map(splitBusiFunc).filter(a => a._1 != "err")

    val stateDstream = wordDstream.updateStateByKey[Int](newUpdateFunc,
      new HashPartitioner(ssc.sparkContext.defaultParallelism), true).cache()
    var counter = 0

    //stateDstream.reduceByKey(_+_).print()
    //stateDstream.print()

    stateDstream.foreachRDD(part => {
      part.foreach {
        case (k, c) => {
          println("time[" + k + "] --> " + c)
        }
      }
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
