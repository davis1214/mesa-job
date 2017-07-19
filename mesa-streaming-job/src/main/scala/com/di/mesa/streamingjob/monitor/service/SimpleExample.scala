package com.di.mesa.streamingjob.monitor.service

import java.util
import com.di.mesa.streamingjob.monitor.util.SplitUtil
import kafka.serializer.StringDecoder
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.{HashPartitioner, SparkConf}

import scala.collection.mutable.HashSet

/**
  * Created by davihe on 2016/1/19.
  * much help from http://apache-spark-user-list.1001560.n3.nabble.com/Writing-to-RabbitMQ-td11283.html
  */
object SimpleExample {
  def main(args: Array[String]): Unit = {

    val updateFunc = (values: Seq[Int], state: Option[Int]) => {
      val currentCount = values.sum
      val previousCount = state.getOrElse(0)
      Some(currentCount + previousCount)
    }

    val ordinary = (('a' to 'z') ++ ('A' to 'Z') ++ ('0' to '9')).toSet

    val newUpdateFunc = (iterator: Iterator[(String, Seq[Int], Option[Int])]) => {
      iterator.flatMap(t => updateFunc(t._2, t._3).map(s => (t._1, s)))
    }

    /**
      * Extracts the hashtags out of a tweet (english only)
 *
      * @param tweet the tweet as [[String]]
      * @return [[HashSet]] of [[String]] containing the hashtags
      */
    def hashTagCleanFunc(tweet: String): HashSet[String] = {
      var x = new StringBuilder
      val y = HashSet[String]()
      var inHash = false
      for (c <- tweet) {
        if (inHash && !ordinary.contains(c)) {
          if (x.length > 1) y += x.toString()
          inHash = false
          x = new StringBuilder
        } else if (inHash && ordinary.contains(c)) {
          x += c.toUpper
        }
        if (c == '#') {
          x += c
          inHash = true
        }
      }
      if (x.length > 1) {
        y += x.toString()
      }
      return y
    }

    val sparkConf = new SparkConf().setAppName("KafkaTwitterTrending").setMaster("local")
      //.setSparkHome("/usr/scala")
    // sparkConf.setMaster("local[4]")

    val ssc = new StreamingContext(sparkConf, Seconds(30))
    //ssc.checkpoint("/user/bd-warehouse/mydir/test/out/checkpoint/")
    val zkQuorum = "10.16.10.76:2181,10.16.10.94:2181,10.16.10.99:2181,10.16.34.187:2181,10.16.34.166:2181,10.16.34.159:2181,10.16.34.58:2181/kafka-0.8.1"
    val brokerList = "10.16.10.196:8092,10.16.10.197:8092,10.16.10.198:8092,10.16.10.199:8092,10.16.10.200:8092,10.16.10.163:8092,10.16.10.164:8092,10.16.10.165:8092,10.16.10.172:8092,10.16.43.148:8092,10.16.43.149:8092,10.16.43.150:8092,10.16.43.151:8092,10.16.43.152:8092,10.16.43.156:8092,10.16.43.153:8092,10.16.43.154:8092,10.16.43.155:8092"

    val kafkaParams = Map[String, String](
      ("metadata.broker.list" -> brokerList),
      ("zookeeper.connect", zkQuorum),
      "group.id" -> "test")

    val topicsSet = "tvadmclick".split(",").toSet
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)
    //kc.createDirectStream[String, String, kafka.serializer.StringDecoder, kafka.serializer.StringDecoder](ssc, topicsSet)

    val lines = messages.map(_._2).filter(a => {
      !a.contains("&vp=e") && a.contains("&b=")
    })

    /*val words = lines.map(line=> {
      try {
        val logs = line.split(SplitUtil.SPLIT)
        val timeStr = SplitUtil.parseLogDate(logs(1))
        val logParam = SplitUtil.parseUrlParam(logs(4))
        val bookpackageId = logParam.get("b")
        val platId = logParam.get("plat")
        if (bookpackageId != None && platId != None) {
          //(bookpackageId.get + SplitUtil.ROW_KEY_SPLIT + timeStr + SplitUtil.ROW_KEY_SPLIT + platId.get, 1)
          (timeStr,1)
        } else {
          ("err", 0)
        }
      } catch {
        case e: Exception => ("err", 0)
      }
    })*/

    //val words = lines.flatMap(hashTagCleanFunc)
    val wordDstream = lines.map(splitBusiFunc).filter(a => a._1 != "err")

    val stateDstream = wordDstream.updateStateByKey[Int](newUpdateFunc,
      new HashPartitioner(ssc.sparkContext.defaultParallelism), true).cache()
    var counter = 0


    stateDstream.foreachRDD((key,value) => {
      println("time["+key+"] --> " + value)

     /* val producer = SKafkaProducer.getInstance()
      counter += 1

      r.foreach(u => {
      })

      if (counter >= 20) {
        producer.send(new ProducerRecord[String, String]("kindle", r.sortBy(_._2, false).take(10).mkString(",")))
        counter = 0
      } else {
        producer.send(new ProducerRecord[String, String]("kindle", r.sortBy(_._2, false).collect().mkString(",")))
      }*/

    })
    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }

  def splitBusiFunc(line: String): (String, Int) = {
    try {
      val logs = line.split(SplitUtil.SPLIT)
      val timeStr = SplitUtil.parseLogDate(logs(1))
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

  import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig}
  import org.apache.kafka.common.serialization.StringSerializer

  object SKafkaProducer {

    @transient private var instance: KafkaProducer[String, String] = _

    def getInstance(): KafkaProducer[String, String] = {
      if (instance == null) {
        val bsServers = new util.ArrayList[String]()
        bsServers.add("10.16.10.174:8092,10.16.10.174:8093,10.16.10.174:8094")

        val kafkaProducerParams = new util.HashMap[String, java.lang.Object]()
        kafkaProducerParams.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bsServers)
        instance = new KafkaProducer[String, String](kafkaProducerParams, new StringSerializer, new StringSerializer)
      }
      instance
    }
  }

}

