package com.di.mesa.streamingjob.kafka2Hbase

import _root_.kafka.producer.{KeyedMessage, Producer}

class KafkaProducer {


  def writeToKafka(topic: String, message: String, producer: Producer[String, String]): Unit = {
    val data: KeyedMessage[String, String] = new KeyedMessage[String, String](topic, message)
    producer.send(data)
  }
}