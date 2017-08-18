package com.di.mesa.job.jstorm.topology

import com.di.mesa.job.jstorm.blot.LocalFileSinkBolt
import com.di.mesa.job.jstorm.configure.MesaConfigure
import com.di.mesa.plugin.rabbitmq.storm.spout.RabbitMQSpout
import com.di.mesa.plugin.storm.bolt.MesaBoltConfiure
import com.di.mesa.plugin.storm.spout.MesaSpoutConfigure
import com.di.mesa.plugin.zookeeper.ZookeeperConfigure
import com.di.mesa.plugin.zookeeper.storm.spout.ZookeeperSpout
import org.slf4j.{Logger, LoggerFactory}

/**
  * Created by Davi on 17/8/17.
  */
class ConfigChangeTopology extends MesaBaseTopology {

  @transient lazy private val LOG: Logger = LoggerFactory.getLogger(getClass)
  private var zkParallelism: Int = 2
  private var localFileSinkBoltParallelism: Int = 2

  @throws(classOf[Exception])
  override def prepareTopologyConfig(args: Array[String]): Unit = {
    super.prepareTopologyConfig(args)

    // cluster config
    workers = 2

    this.config.put(MesaBoltConfiure.SHOULD_RECORD_METRIC_TO_OPENTSDB, "true")
    this.config.put(MesaBoltConfiure.OPENTSDB_URL, "http://10.8.96.120:4242,http://10.8.96.121:4242,http://10.8.96.122:4242")

    //common config
    this.config.put(MesaSpoutConfigure.MESA_STREAMING_ID, ZookeeperConfigure.ZOOKEEPER_STREAMING_ID);

    //enable zk notifier
    this.config.enableZkNotifer(true)
    this.config.setNotiferStreamingId(ZookeeperConfigure.ZOOKEEPER_STREAMING_ID_DEFAULT)
    this.config.setNotiferComponentId(ZookeeperConfigure.ZOOKEEPER_COMPONENT_ID_DEFAULT)
    this.config.put(ZookeeperConfigure.ZOOKEEPER_MESA_SERVERS, "127.0.0.1:2181");
    this.config.put(ZookeeperConfigure.ZOOKEEPER_ROOT_NODE_PATH, "/mesa/conf");
    this.config.put(ZookeeperConfigure.ZOOKEEPER_CONF_FILE, "mesa.properties");
    this.config.put(ZookeeperConfigure.ZOOKEEPER_MESA_CONN_TIMEOUT, "100000");

    if (config.isLocalMode) {
      workers = 1
      zkParallelism = 1
      localFileSinkBoltParallelism = 1
    }

  }

  @throws(classOf[Exception])
  override def prepareTopologyBuilder(): Unit = {
    super.prepareTopologyBuilder

    //config tick spout
    topologyBuilder.setSpout(MesaConfigure.TICK_SPOUT_NAME, getTickSpout, 1)

    //config sink
    topologyBuilder.setBolt(classOf[LocalFileSinkBolt].getSimpleName, new LocalFileSinkBolt, localFileSinkBoltParallelism)
      .allGrouping(MesaConfigure.TICK_SPOUT_NAME, "count")
  }

}

object ConfigChangeTopology {

  def main(args: Array[String]): Unit = {
    new ToolRunner().run(new ConfigChangeTopology, args);
  }

}

