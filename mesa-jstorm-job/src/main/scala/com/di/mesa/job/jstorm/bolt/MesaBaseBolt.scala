package com.di.mesa.job.jstorm.bolt

import java.util
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{ConcurrentHashMap, TimeUnit}

import backtype.storm.task.{OutputCollector, TopologyContext}
import backtype.storm.topology.OutputFieldsDeclarer
import backtype.storm.topology.base.BaseRichBolt
import backtype.storm.tuple.Tuple
import com.di.mesa.common.opentsdb.ShuffledOpentsdbClient
import com.di.mesa.common.opentsdb.builder.{Metric, MetricBuilder}
import com.google.common.cache.{CacheBuilder, CacheLoader, LoadingCache}
import org.slf4j.{Logger, LoggerFactory}

/**
  * Created by Administrator on 17/7/19.
  *
  */
class MesaBaseBolt extends BaseRichBolt {

  private val logger: Logger = LoggerFactory.getLogger(classOf[MesaBaseBolt])
  protected var meticCounter: ConcurrentHashMap[String, AtomicLong] = null
  protected val parserCost: String = "parserCost"
  protected val TupleCount: String = "TupleCount"
  protected val HbasePut: String = "HbasePut"
  protected val ConstructHbasePut: String = "ConstructHbasePut"
  protected val TableFlushCommits: String = "TableFlushCommit"
  protected val TableFlushCount: String = "TableFlushCount"
  protected val ExecuteCost: String = "ExecuteCost"
  protected val PutCount: String = "PutCount"
  protected var lastTime: AtomicLong = new AtomicLong(0l)
  protected var lastPrintTime: AtomicLong = new AtomicLong(0l)
  protected var shouldRecordToOpentsdb: Boolean = false
  protected var opentsdbClient: ShuffledOpentsdbClient = null
  protected var stormConf: util.Map[_, _] = null
  private var cache: LoadingCache[String, String] = null


  override def prepare(stormConf: util.Map[_, _], context: TopologyContext, collector: OutputCollector): Unit = {
    this.stormConf = stormConf

    meticCounter = new ConcurrentHashMap[String, AtomicLong]
    lastPrintTime.set(System.currentTimeMillis)
    lastTime.set(System.currentTimeMillis)

    shouldRecordToOpentsdb = false
    if (stormConf.containsKey("busi.record.monitor.to.opentsdb")) {
      shouldRecordToOpentsdb = stormConf.get("busi.record.monitor.to.opentsdb").toString.toBoolean
    }

    if (shouldRecordToOpentsdb) {
      val opentsdbUrl: String = stormConf.get("busi.record.monitor.opentsdb.url").toString
      opentsdbClient = new ShuffledOpentsdbClient(opentsdbUrl)
    }

    cache = CacheBuilder.newBuilder.maximumSize(1000).expireAfterWrite(10, TimeUnit.MINUTES).build(new CacheLoader[String, String]() {
      def load(s: String): String = {
        return null
      }
    })
  }


  protected def recordCounter(monitorCounter: ConcurrentHashMap[String, AtomicLong], metricMonitor: String) {

    val monitorKey = metricMonitor match {
      case monitorKey: String => monitorKey
      case _ => ""
    }

    if (!monitorCounter.containsKey(monitorKey)) {
      monitorCounter.put(monitorKey, new AtomicLong(0l))
    }
    monitorCounter.get(monitorKey).getAndIncrement
  }

  protected def recordCounter(monitorCounter: ConcurrentHashMap[String, AtomicLong], metricMonitor: String, total: Long) {

    val monitorKey = metricMonitor match {
      case monitorKey: String => monitorKey
      case _ => ""
    }

    if (!monitorCounter.containsKey(monitorKey)) {
      monitorCounter.put(monitorKey, new AtomicLong(0l))
    }
    monitorCounter.get(monitorKey).getAndAdd(total)
  }

  protected def recordMonitorLog {
    if ((System.currentTimeMillis - lastPrintTime.get) > 60 * 1000) {
      logger.info("taskIndex " + Thread.currentThread.getName + " , emitMeticCounter " + meticCounter.toString)
      if (shouldRecordToOpentsdb) {
        val builder: MetricBuilder = MetricBuilder.getInstance
        val metricName: String = stormConf.get("topName").toString
        val timestamp: Long = System.currentTimeMillis / 1000
        val keys: util.Enumeration[String] = meticCounter.keys
        while (keys.hasMoreElements) {
          val key: String = keys.nextElement
          val value: Long = meticCounter.get(key).get
          val tags: util.Map[String, String] = getBasicMetricTags
          tags.put(key, key)
          val newMetric: Metric = new Metric(metricName, timestamp, value, tags)
          builder.addMetric(newMetric)
        }
        opentsdbClient.putData(builder)
      }
      meticCounter.clear
      lastPrintTime.set(System.currentTimeMillis)
    }
  }

  protected def getBasicMetricTags: java.util.Map[String, String] = {
    val tags: util.HashMap[String, String] = new util.HashMap[String, String]
    tags.put("bolt.name", this.getClass.getSimpleName)
    tags.put("thread", Thread.currentThread.getName)
    return tags
  }


  override def execute(input: Tuple): Unit = {}


  override def declareOutputFields(declarer: OutputFieldsDeclarer): Unit = {
    this.declareOutputFields(OutputFieldsDeclarer)
  }
}
