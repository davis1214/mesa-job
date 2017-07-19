package com.di.mesa.streamingjob.monitor.rts

import com.di.mesa.streamingjob.monitor.manager.KafkaManager

/**
  * Created by Administrator on 16/5/19.
  */
class InputStreamHolder {


  def getInputStream(): Unit ={
    //TODO 选择数据源,做下封装
//    val kafkaParam = Map[String, String](
//      "zookeeper.connect" -> zkQuorum,
//      "metadata.broker.list" -> brokerlist,
//      "group.id" -> groupId,
//      "auto.offset.reset" -> kafka.api.OffsetRequest.LargestTimeString,
//      "client.id" -> groupId,
//      "zookeeper.connection.timeout.ms" -> "10000"
//    )
//
//    val topicsSet = topic.split(",").toSet
//    val kc = new KafkaManager(kafkaParam)
//    val lines = kc.createDirectStream[String, String, kafka.serializer.StringDecoder, kafka.serializer.StringDecoder](ssc, topicsSet)

  }

}
