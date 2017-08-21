package com.di.mesa.job.jstorm.topology

import java.util

import backtype.storm.topology.TopologyBuilder
import backtype.storm.utils.Utils
import backtype.storm.{LocalCluster, StormSubmitter}
import com.di.mesa.job.jstorm.configure.{VdianMqConfigure, MesaConfigure}
import com.di.mesa.job.jstorm.spout.{VdianMQSpout, TickSpout}
import com.di.mesa.plugin.rabbitmq.RabbitmqConfigure
import com.di.mesa.plugin.rabbitmq.storm.spout.RabbitMQSpout
import com.di.mesa.plugin.storm.{CommonConfiure, MesaTopologyBuilder}
import com.di.mesa.plugin.zookeeper.storm.spout.ZookeeperSpout
import com.weidian.di.storm.mesa.storm.constant.CommonConfiure
import com.weidian.di.storm.mesa.storm.constant.CommonConfiure
import com.weidian.di.storm.mesa.storm.constant.VdianMqConfigure
import com.weidian.di.storm.mesa.storm.spout.VdianMQSpout
import com.weidian.di.storm.mesa.storm.spout.VdianMQSpout
import org.slf4j.{Logger, LoggerFactory}

/**
  * Created by davi on 17/8/3.
  */
private[mesa] trait MesaBaseTopology extends Tool {

  @transient lazy private val LOG: Logger = LoggerFactory.getLogger(getClass)

  var topologyBuilder: MesaTopologyBuilder = null
  var workers: Int = 4

  var shouldEnableZkNotifer: Boolean = false;

  @throws(classOf[Exception])
  def prepareTopologyConfig(args: Array[String]): Unit = {

    if (args.length <= 0) {
      //      config.put(MesaConfigure.TOPOLOGY_NAME, getClass.getSimpleName)
      //      config.put(MesaConfigure.RUNNING_MODE, MesaConfigure.RUNNING_MODE_LOCAL)
      config.setTopologyName(getClass.getSimpleName)
      config.setRunningMode(MesaConfigure.RUNNING_MODE_LOCAL)
      config.setDebug(false);
    } else {
      val topologyName = if (args.length > 0) args(0) else getClass.getSimpleName
      val runningMode = if (args.length > 1) args(1) else "local"
      /* config.put(MesaConfigure.TOPOLOGY_NAME, topologyName)
       config.put(MesaConfigure.RUNNING_MODE, runningMode.toLowerCase()) //default CommonConfigure.RUNNING_MODE_CLUSTER*/
      config.setTopologyName(topologyName)
      config.setRunningMode(runningMode.toLowerCase())

    }

    config.setMaxSpoutPending(1000000);
    config.setMessageTimeoutSecs(90);
  }

  @throws(classOf[Exception])
  def prepareTopologyBuilder(): Unit = {
    LOG.info("prepareTopologyBuilder ,shouldEnableZkNotifer ", shouldEnableZkNotifer)
    topologyBuilder = new MesaTopologyBuilder(config)

    if (config.shouldEnableNotifer()) {
      val zkParallelism = 1
      //MesaSpoutConfigure.MESA_STREAMING_ID
      topologyBuilder.setSpout(config.getNotiferComponentId, new ZookeeperSpout(config.getNotiferStreamingId), zkParallelism)
    }
  }

  @throws(classOf[Exception])
  def startTopology(): Unit = {
    if (config.isClusterMode) {
      config.setNumWorkers(workers)
      StormSubmitter.submitTopology(config.getTopologyName, config, topologyBuilder.createTopology)
    }
    else {
      config.setNumWorkers(workers)
      val localCluster: LocalCluster = new LocalCluster
      localCluster.submitTopology(config.getTopologyName, config, topologyBuilder.createTopology)
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

  protected def getVdianMQSpout: VdianMQSpout = {
    var zkAddress: String = VdianMqConfigure.VMQ_DAILY_ADDRESS
    if (config.isClusterMode) {
      zkAddress = VdianMqConfigure.VMQ_ONLINE_ADDRESS
    }

    var topic: String = "test"
    if (config.containsKey(VdianMqConfigure.VDIAN_MQ_SUBSCRIBED_TOPIC_NAME)) {
      topic = config.get(VdianMqConfigure.VDIAN_MQ_SUBSCRIBED_TOPIC_NAME).toString
    }
    var groupName: String = "di_group_name"
    if (config.containsKey(VdianMqConfigure.VDIAN_MQ_SUBSCRIBED_TOPIC_GROUPNAME)) {
      groupName = config.get(VdianMqConfigure.VDIAN_MQ_SUBSCRIBED_TOPIC_GROUPNAME).toString
    }

    return new VdianMQSpout(60, topic, zkAddress, groupName)
  }

  //def isLocalMode: Boolean = config.isLocalMode

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
