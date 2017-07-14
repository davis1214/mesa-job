package com.di.mesa.sparkjob.util

import com.esotericsoftware.kryo.Kryo
import org.apache.spark.serializer.KryoRegistrator
import org.apache.spark.{SparkConf, SparkContext}

class MyRegistrator extends KryoRegistrator {
  override def registerClasses(kryo: Kryo) {
  }
}

object SparkCommon {
  val conf = new SparkConf()

  conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  conf.set("spark.kryo.registrator", "com.di.mesa.sparkjob.util.MyRegistrator")
  conf.set("spark.kryoserializer.buffer", "1g")
  conf.set("spark.kryoserializer.buffer.max", "2047m")
  conf.set("spark.driver.maxResultSize", "20g")
  conf.set("spark.speculation", "true")
  conf.set("spark.akka.frameSize", "500")
  conf.set("spark.akka.timeout", "1000")
  conf.set("spark.rpc.askTimeout", "1000")
  conf.set("spark.task.maxFailures", "400")
  conf.set("spark.shuffle.consolidateFiles", "true")
  conf.set("spark.shuffle.spill", "true")

  conf.set("spark.eventLog.enabled", "true")
  conf.set("spark.executor.memory", "5g")

  def getClusterSparkContext = new SparkContext(conf)

  def getLocalSparkContext = {
    conf.setAppName("Wow").setMaster("local")
    conf.setMaster("local")
    conf.setAppName("local-test")
    conf.set("spark.driver.allowMultipleContexts", "true")

    new SparkContext(conf)
  }

  def getSparkConf: SparkConf = conf

}
