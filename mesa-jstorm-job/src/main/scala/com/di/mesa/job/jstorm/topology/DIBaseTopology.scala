package com.di.mesa.job.jstorm.topology

import java.util

import backtype.storm.topology.TopologyBuilder
import backtype.storm.utils.Utils
import backtype.storm.{LocalCluster, StormSubmitter}
import com.di.mesa.job.jstorm.configure.{MesaConfigure, RabbitmqConfigure}
import com.di.mesa.job.jstorm.spout.{RabbitMQSpout, TickSpout}
import org.slf4j.{Logger, LoggerFactory}

/**
  * Created by davi on 17/8/3.
  */
private[mesa] trait DIBaseTopology extends Tool {

  @transient lazy private val LOG: Logger = LoggerFactory.getLogger(getClass)

  var topologyBuilder: TopologyBuilder = null
  var workers: Int = 4

  @throws(classOf[Exception])
  def prepareTopologyConfig(args: Array[String]): Unit = {

    if (args.length <= 0) {
      config.put(MesaConfigure.TOPOLOGY_NAME, getClass.getSimpleName)
      config.put(MesaConfigure.RUNNING_MODE, MesaConfigure.RUNNING_MODE_LOCAL)
      config.setDebug(false);
    } else {
      val topologyName = if (args.length > 0) args(0) else getClass.getSimpleName
      val runningMode = if (args.length > 1) args(1) else "local"
      config.put(MesaConfigure.TOPOLOGY_NAME, topologyName)
      config.put(MesaConfigure.RUNNING_MODE, runningMode.toLowerCase()) //default CommonConfigure.RUNNING_MODE_CLUSTER
    }

    config.setMaxSpoutPending(1000000);
    config.setMessageTimeoutSecs(90);
  }

  @throws(classOf[Exception])
  def prepareTopologyBuilder(): Unit = {
    LOG.info("prepareTopologyBuilder")
    topologyBuilder = new TopologyBuilder
  }

  @throws(classOf[Exception])
  def startTopology(): Unit = {
    if (!isLocalMode) {
      config.setNumWorkers(workers)
      StormSubmitter.submitTopology(config.getString(MesaConfigure.TOPOLOGY_NAME), config, topologyBuilder.createTopology)
    }
    else {
      val localCluster: LocalCluster = new LocalCluster
      localCluster.submitTopology(config.getString(MesaConfigure.TOPOLOGY_NAME), config, topologyBuilder.createTopology)
      Utils.sleep(1000 * 60 * 4)
      localCluster.shutdown
    }
  }

  @throws(classOf[Exception])
  override def run(args: Array[String]): Unit = {
    prepareTopologyConfig(args)
    prepareTopologyBuilder
    startTopology
  }


  def isLocalMode: Boolean = {
    val runningMode = config.getStringOrDefault(MesaConfigure.RUNNING_MODE, MesaConfigure.RUNNING_MODE_LOCAL)
    runningMode.equals(MesaConfigure.RUNNING_MODE_LOCAL)
  }

  protected def getTickSpout: TickSpout = {
    val tickMap = new util.HashMap[String, Integer]
    tickMap.put("count", 15)
    tickMap.put("persist", 30)
    new TickSpout(tickMap, 1)
  }


  //vhost : String, queueName : String, host : String, userName : String, password : String, port : String, routeKey : String, busiMsgType : String
  protected def getRabbitMQSpout: RabbitMQSpout = new RabbitMQSpout(config.getString(RabbitmqConfigure.VHOST_MARKER), config.getString(RabbitmqConfigure.QUEUE_NAME_MARKER)
    , config.getString(RabbitmqConfigure.HOST_MARKER), config.getString(RabbitmqConfigure.USER_NAME_MARKER), config.getString(RabbitmqConfigure.PASSWD_MARKER),
    config.getString(RabbitmqConfigure.PORT_MARKER), config.getString(RabbitmqConfigure.ROUTEKEY_MARKER), RabbitmqConfigure.RABBITMQ_DEFAULT_STREAM_ID)

}
