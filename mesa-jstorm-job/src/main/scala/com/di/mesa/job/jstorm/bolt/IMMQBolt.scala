package com.di.mesa.job.jstorm.bolt

import java.util
import java.util.{Map => JMap}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}

import backtype.storm.Config
import backtype.storm.task.{OutputCollector, TopologyContext}
import backtype.storm.topology.OutputFieldsDeclarer
import backtype.storm.topology.base.BaseRichBolt
import backtype.storm.tuple.{Fields, Tuple}
import com.di.mesa.job.jstorm.utils.{TupleHelpers, IMHashMap}
import com.google.gson.{Gson, GsonBuilder}
import com.vdian.vdianmq.client.{MqException, MqClientImpl}
import com.vdian.vdianmq.client.producer.MqProducerImpl
import com.vdian.vdianmq.model.{ResponseCode, Message}
import com.vdian.vdianmq.model.remoting.SendResult
import org.apache.log4j.Logger

import scala.collection.mutable.ArrayBuffer

/**
  * Created by Administrator on 17/6/28.
  */
class IMMQBolt(zkAddress: String, tag: String) extends BaseRichBolt {

  @transient lazy private val LOG: Logger = Logger.getLogger(classOf[IMMQBolt])

  var collector: OutputCollector = _
  var producer: MqProducerImpl = _
  var mapper: Gson = _

  var counter: AtomicLong = _

  var imMap: IMHashMap = _

  var tmpMap = scala.collection.mutable.HashMap[String, String]()

  var lastTime :Long = _
  var shouldFlush: AtomicBoolean = _

  //TODO 增加对垒来处理
  override def execute(tuple: Tuple): Unit = {

    if (!TupleHelpers.isTickTuple(tuple)) {
      imMap.update(tuple.getString(0), tuple.getString(1))
      this.collector.ack(tuple)
    } else {
      LOG.info(s"wow ,it's tick time ${Thread.currentThread().getName}")
      case class UserAction(name: String, time: String)

      var objectArray = new ArrayBuffer[UserAction](100)

      imMap.getMap.foreach(f => {

        objectArray += UserAction(f._1, f._2)

        if ((System.currentTimeMillis() - lastTime) > 30 * 1000) {
          lastTime = System.currentTimeMillis
          shouldFlush.set(true)
        }

        if (objectArray.size >= 100 || shouldFlush.get()) {
          shouldFlush.set(false)
          val mmsgs = mapper.toJson(objectArray.toArray)

          def buildMessage(mmsgs: String): Message = {
            val message: Message = new Message("user_action")
            message.setBody(mmsgs.getBytes)
            message.addTag(tag)
            message
          }

          def sendMsg: Boolean = {
            var isSuccess = true

            try {
              val message: Message = buildMessage(mmsgs)
              val sendResult: SendResult = producer.send(message)

              sendResult.isSuccess() match {
                case true => {
                  LOG.info("send succeed, messageId: ".concat(sendResult.getMessageIdString()))
                }
                case _ => {
                  isSuccess = false
                  LOG.error("send failed, error desc ".concat(ResponseCode.getDesc(sendResult.getCode())))
                }
              }

            } catch {
              case mqException: MqException => {
                counter.getAndIncrement()
                LOG.error(mqException.getMessage, mqException)

                if (counter.get() > 2) {
                  prepareMQProducer

                  counter.set(0)
                }

                isSuccess = false
              }
              case e: Exception => {
                LOG.error(e.getMessage, e)
                isSuccess = false
              }
            }
            isSuccess
          }

          val isSuccess = sendMsg

          if (!isSuccess) {
            objectArray.foreach(f => {
              tmpMap += (f.name -> f.time)
            })
          }
          objectArray.clear()
        }
      })

      imMap.getMap.clear
      imMap.getMap ++ tmpMap
    }
  }

  override def prepare(stormConf: util.Map[_, _], context: TopologyContext, collector: OutputCollector): Unit = {
    this.collector = collector

    prepareMQProducer

    val builder = new GsonBuilder();
    mapper = builder.create();

    imMap = new IMHashMap

    lastTime = System.currentTimeMillis()
    shouldFlush = new AtomicBoolean(false)
  }

  def prepareMQProducer: Unit = {
    val mqClient = new MqClientImpl()
    mqClient.setZkAddress(zkAddress)
    mqClient.init()

    producer = new MqProducerImpl
    producer.setMqClient(mqClient)
    producer.init()
  }

  override def declareOutputFields(declarer: OutputFieldsDeclarer): Unit = {
    declarer.declare(new Fields("word"))
  }

  override def getComponentConfiguration: JMap[String, AnyRef] = {
    val conf = new util.HashMap[String, Object] {}
    val frq: Integer = 60
    conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, frq)
    return conf
  }

}
