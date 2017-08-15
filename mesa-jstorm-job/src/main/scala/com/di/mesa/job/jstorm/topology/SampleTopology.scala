package com.di.mesa.job.jstorm.topology

import com.di.mesa.job.jstorm.blot.LocalFileSinkBolt
import com.di.mesa.job.jstorm.configure.{CommonConfiure, MesaConfigure, RabbitmqConfigure}
import com.di.mesa.job.jstorm.spout.RabbitMQSpout
import org.slf4j.{Logger, LoggerFactory}


/**
  * Created by davi on 17/8/4.
  */
class SampleTopology extends DIBaseTopology {
  @transient lazy private val LOG: Logger = LoggerFactory.getLogger(getClass)
  private var rabbitMQParallelism: Int = 2
  private var localFileSinkBoltParallelism: Int = 2

  @throws(classOf[Exception])
  override def prepareTopologyConfig(args: Array[String]): Unit = {
    super.prepareTopologyConfig(args)

    // cluster config
    workers = 2

    this.config.put(RabbitmqConfigure.EXCHANGE_MARKER, "x-order-direct")
    this.config.put(RabbitmqConfigure.EXCHANGETYPE_MARKER, "direct")
    this.config.put(RabbitmqConfigure.TRANSACTION_MARKER, "true")
    this.config.put(RabbitmqConfigure.DURABLE_MARKER, "true")

    this.config.put(RabbitmqConfigure.VHOST_MARKER, "/order")
    this.config.put(RabbitmqConfigure.HOST_MARKER, "192.168.16.61")
    this.config.put(RabbitmqConfigure.PORT_MARKER, "5673")
    this.config.put(RabbitmqConfigure.USER_NAME_MARKER, "test")
    this.config.put(RabbitmqConfigure.PASSWD_MARKER, "test123456")
    this.config.put(RabbitmqConfigure.QUEUE_NAME_MARKER, "order.all.item.refund.status.di")

    this.config.put(CommonConfiure.SHOULD_RECORD_METRIC_TO_OPENTSDB, "true")
    this.config.put(CommonConfiure.OPENTSDB_URL, "http://10.8.96.120:4242,http://10.8.96.121:4242,http://10.8.96.122:4242")

    if (isLocalMode) {
      workers = 1
      rabbitMQParallelism = 1
      localFileSinkBoltParallelism = 1

      this.config.put(RabbitmqConfigure.VHOST_MARKER, "/order")
      this.config.put(RabbitmqConfigure.HOST_MARKER, "10.1.16.61")
      this.config.put(RabbitmqConfigure.PORT_MARKER, "5673")
      this.config.put(RabbitmqConfigure.USER_NAME_MARKER, "test")
      this.config.put(RabbitmqConfigure.PASSWD_MARKER, "test123456")
      this.config.put(RabbitmqConfigure.QUEUE_NAME_MARKER, "q.order.all.item.refund.status.di")
      this.config.put(RabbitmqConfigure.ROUTEKEY_MARKER, "")
    }

  }

  @throws(classOf[Exception])
  override def prepareTopologyBuilder(): Unit = {
    super.prepareTopologyBuilder

    //config tick spout
    topologyBuilder.setSpout(MesaConfigure.TICK_SPOUT_NAME, getTickSpout, 1)

    //config datasource
    topologyBuilder.setSpout(classOf[RabbitMQSpout].getSimpleName, getRabbitMQSpout, rabbitMQParallelism)

    //config sink
    topologyBuilder.setBolt(classOf[LocalFileSinkBolt].getSimpleName, new LocalFileSinkBolt, localFileSinkBoltParallelism)
      .shuffleGrouping(classOf[RabbitMQSpout].getSimpleName, RabbitmqConfigure.RABBITMQ_DEFAULT_STREAM_ID)
      .allGrouping(MesaConfigure.TICK_SPOUT_NAME, "count")
  }

}


object SampleTopology {

  def main(args: Array[String]): Unit = {
    new ToolRunner().run(new SampleTopology, args);
  }

}


