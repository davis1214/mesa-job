package com.di.mesa.streamingjob.monitor.service

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, Duration, StreamingContext}

object KafkaSparkDemoMain {

  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("kafka-spark-demo").setMaster("local")
    val scc = new StreamingContext(sparkConf, Seconds(30))
    scc.checkpoint("hdfs://10.10.52.98:9000/user/data/checkpoint/spark-streaming/tvadmclick") // 因为使用到了updateStateByKey,所以必须要设置checkpoint

    val groupId = "test"
    val topics = Set("tvadmpv") //我们需要消费的kafka数据的topic
    val zkQuorum = "10.16.10.76:2181,10.16.10.94:2181,10.16.10.99:2181,10.16.34.187:2181,10.16.34.166:2181,10.16.34.159:2181,10.16.34.58:2181/kafka-0.8.1"
    val brokerList = "10.16.10.196:8092,10.16.10.197:8092,10.16.10.198:8092,10.16.10.199:8092,10.16.10.200:8092,10.16.10.163:8092,10.16.10.164:8092,10.16.10.165:8092,10.16.10.172:8092,10.16.43.148:8092,10.16.43.149:8092,10.16.43.150:8092,10.16.43.151:8092,10.16.43.152:8092,10.16.43.156:8092,10.16.43.153:8092,10.16.43.154:8092,10.16.43.155:8092"

    val kafkaParam = Map(
      "zookeeper.connect" -> zkQuorum,
      "metadata.broker.list" -> brokerList,
      "group.id" -> groupId,
      "auto.offset.reset" -> kafka.api.OffsetRequest.LargestTimeString,
      "client.id" -> groupId,
      "zookeeper.connection.timeout.ms" -> "10000"
    )

    val stream: InputDStream[(String, String)] = createStream(scc, kafkaParam, topics)

    val words = stream.map(_._2) // 取出value
      .flatMap(_.split(",")) // 将字符串使用空格分隔
      .map(r => (r, 1)) // 每个单词映射成一个pair

    words
      .updateStateByKey[Int](updateFunc) // 用当前batch的数据区更新已有的数据
      .print() // 打印前10个数据

    scc.start() // 真正启动程序
    scc.awaitTermination() //阻塞等待
  }

  val updateFunc = (currentValues: Seq[Int], preValue: Option[Int]) => {
    val curr = currentValues.sum
    val pre = preValue.getOrElse(0)
    Some(curr + pre)
  }

  /**
    * 创建一个从kafka获取数据的流.
    *
    * @param scc        spark streaming上下文
    * @param kafkaParam kafka相关配置
    * @param topics     需要消费的topic集合
    * @return
    */
  def createStream(scc: StreamingContext, kafkaParam: Map[String, String], topics: Set[String]) = {
    KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](scc, kafkaParam, topics)
  }
}