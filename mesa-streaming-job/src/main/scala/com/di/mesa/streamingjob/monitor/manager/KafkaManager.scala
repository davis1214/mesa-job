package com.di.mesa.streamingjob.monitor.manager

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.Decoder
import org.apache.spark.SparkException
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{KafkaUtils, HasOffsetRanges, KafkaCluster}
import org.apache.spark.streaming.kafka.KafkaCluster.LeaderOffset
import scala.reflect.ClassTag

/**
  * Created by davihe on 2016/3/3.
  */
class KafkaManager(val kafkaParams: Map[String, String]) extends Serializable {
  private val kc = new KafkaCluster(kafkaParams)


  def createDirectStream[K: ClassTag, V: ClassTag, KD <: Decoder[K] : ClassTag, VD <: Decoder[V] : ClassTag](ssc: StreamingContext, topics: Set[String]): InputDStream[(K, V)] = {
    val groupId = kafkaParams.get("group.id").get
    //在读取数据前，根据实际情况更新ZK上offsets
    setOrUpdateOffsets(topics, groupId)

    //从zk上读取message
    val messages = {
      val partationsE = kc.getPartitions(topics)
      if (partationsE.isLeft) throw new SparkException("get kafka partation failed ")
      val partations = partationsE.right.get
      val consumerOffsetsE = kc.getConsumerOffsets(groupId, partations)
      if (consumerOffsetsE.isLeft) throw new SparkException("get kafka consumer offsets failed ")
      val consumerOffsets = consumerOffsetsE.right.get

      KafkaUtils.createDirectStream[K, V, KD, VD, (K, V)](ssc, kafkaParams, consumerOffsets, (mm: MessageAndMetadata[K, V]) => (mm.key, mm.message))
    }
    messages
  }

  private def setOrUpdateOffsets(topics: Set[String], groupId: String): Unit = {
    topics.foreach(topic => {
      var hasConsumed = true
      val partitionsE = kc.getPartitions(Set(topic))
      if (partitionsE.isLeft) throw new SparkException("get kafka partition failed")

      val partitions = partitionsE.right.get
      val consumerOffsectE = kc.getConsumerOffsets(groupId, partitions)
      if (consumerOffsectE.isLeft) hasConsumed = false

      if (hasConsumed) {
        //已消费过
        //获取zk上已经小的Offset
        println(s"consumed update offset  ${topics.toString()}")
        val earliestLeaderOffsets = kc.getEarliestLeaderOffsets(partitions).right.get
        val consumerOffsets = consumerOffsectE.right.get
        println(s"kafka smallest offset is ${earliestLeaderOffsets} , consumer offset is ${consumerOffsets}")
        var offsets: Map[TopicAndPartition, Long] = Map()
        consumerOffsets.foreach({
          case (tp, offset) => {
            val earliestLeaderOffsets2 = earliestLeaderOffsets(tp).offset
            if (offset < earliestLeaderOffsets2) {
              //如果已经消费的offset比当前kafka最早的还小
              println(s" topic :${tp.topic}}  partation :${tp.partition}  consumer offset is ${offset}  kafka smallest offset is: ${earliestLeaderOffsets2}")
              offsets += (tp -> earliestLeaderOffsets2)
            }
          }
        })
        if (!offsets.isEmpty) {
          //将消费的已经过时的offset更新到当前kafka中最早的
          kc.setConsumerOffsets(groupId, offsets)
          println(s" update  ${groupId} offsets: ${offsets}")
        }
      } else {
        //没有消费过
        val reset = kafkaParams.get("auto.offset.reset").map(_.toLowerCase)
        var leaderOffsets: Map[TopicAndPartition, LeaderOffset] = null
        if (reset == Some(kafka.api.OffsetRequest.SmallestTimeString)) {
          leaderOffsets = kc.getEarliestLeaderOffsets(partitions).right.get
        } else {
          leaderOffsets = kc.getLatestLeaderOffsets(partitions).right.get
        }

        val offsets = leaderOffsets.map {
          case (tp, lo) => (tp, lo.offset)
        }

        kc.setConsumerOffsets(groupId, offsets)
      }
    })
  }

  def updateZKOffsets(rdd: RDD[(String, String)]): Unit = {
    val groupId = kafkaParams.get("group.id").get
    val offsetList = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

    for (offsets <- offsetList) {
      val topicAndPartition = TopicAndPartition(offsets.topic, offsets.partition)
      val o = kc.setConsumerOffsets(groupId, Map((topicAndPartition, offsets.untilOffset)))
      if (o.isLeft) {
        println(s"Error updating the offset to Kafka cluster: ${o.left.get}")
      }
    }
  }
}
