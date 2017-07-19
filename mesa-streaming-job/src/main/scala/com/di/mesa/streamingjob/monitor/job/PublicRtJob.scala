package com.di.mesa.streamingjob.monitor.job

import org.apache.spark.Accumulator
import org.apache.spark.SparkConf
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import com.di.mesa.streamingjob.monitor.manager.KafkaManager
import com.di.mesa.streamingjob.monitor.common.DatabaseException
import com.di.mesa.streamingjob.monitor.common.TopicEnums
import com.di.mesa.streamingjob.monitor.common.TopicEnums.APP_UDC_ACCOUNT
import com.di.mesa.streamingjob.monitor.common.TopicEnums.TEST
import com.di.mesa.streamingjob.monitor.processor.PublicLogProcessor
import java.util.concurrent.atomic.AtomicBoolean
import org.omg.PortableInterceptor.SYSTEM_EXCEPTION

/**
 * Created by Administrator on 16/6/3.
 *
 * PublicRtJob 在spark-streaming 的实现
 *
 */
object PublicRtJob extends RtJob {

  def main(args: Array[String]) {
    parser.parse(args, defaultParams).map { param =>
      println(param)
      //System.exit(1)
      run(param)
    } getOrElse {
      System.exit(1)
    }
  }

  //TODO 重构下异常的处理
  def run(param: JobParam) {
    try {
      val sc = new SparkConf().setAppName(PublicRtJob.getClass.getSimpleName + "_Job")
      sc.set("spark.streaming.kafka.maxRatePerPartition", param.rate) //每秒钟最大消费
      //sc.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")

      //TODO for test
      if ("test1".equals(param.topic)) {
        sc.setMaster("local")
      }

      val ssc = new StreamingContext(sc, Seconds(param.duration))
      if (!"test1".equals(param.topic))
        ssc.checkpoint("hdfs://argo/user/www/output/udc_test")

      val kafkaParam = buildKakfaParam(param)
      val broadcast: Broadcast[List[Map[String, String]]] = ssc.sparkContext.broadcast(initFilters(param.jobId))
      val topicsSet = param.topic.split(",").toSet
      val kc = new KafkaManager(kafkaParam)

      val lines = kc.createDirectStream[String, String, kafka.serializer.StringDecoder, kafka.serializer.StringDecoder](ssc, topicsSet)
      val Array(lbatchAccumulator, lineAccumulator) = Array(ssc.sparkContext.accumulator(1l, "line-batch-accumulator"), ssc.sparkContext.accumulator(1l, "line-accumulator"))
      lbatchAccumulator += 1

      streamProcess(kc, broadcast, param.topic, lines, lineAccumulator)
      logInfo("line-batch-accumulator:" + lbatchAccumulator.value + " , line-accumulator:" + lineAccumulator)

      ssc.start()
      daemonThread(ssc, param.jobId).start()
      ssc.awaitTermination()
    } catch {
      case e: Exception => logError(e.getMessage, e)
      case _            => println("other")
    }
  }

  def daemonThread(ssc: org.apache.spark.streaming.StreamingContext, jobId: String) = {
    val thread = new Thread(new Runnable() {
      override def run() {
        val shouldStop = new AtomicBoolean(false)
        while (!shouldStop.get) {
          if (!ssc.sparkContext.getConf.contains("spark.app.id")) {
            Thread.sleep(2000)
            logInfo("appid is not esits")
          } else {
            shouldStop.set(true)
            logInfo("application id is ".concat(ssc.sparkContext.getConf.getAppId))
            updateAppIdByJobId(jobId, ssc.sparkContext.getConf.getAppId)
          }
        }
      }
    })
    thread.setDaemon(true)
    thread
  }

  def streamProcess(kc: KafkaManager, broadcast: Broadcast[List[Map[String, String]]], topic: String, lines: InputDStream[(String, String)], lineAccumulator: Accumulator[Long]): Unit = {
    try {
      lines.foreachRDD(rdd => {
        if (!rdd.isEmpty()) {
          TopicEnums.withName(topic) match {
            case APP_UDC_ACCOUNT => PublicLogProcessor.process(broadcast.value, rdd, lineAccumulator)
            case _               => PublicLogProcessor.process(broadcast.value, rdd, lineAccumulator)
          }

          kc.updateZKOffsets(rdd)
        } else {
          logError("Current batch of KafkaRDD is empty.")
        }
      })
    } catch {
      case e: DatabaseException => logError(e.getMessage, e)
      case _                    => logError("other exceptions")
    }
  }

}
