package com.di.mesa.job.jstorm.topology

import backtype.storm.tuple.Fields
import com.di.mesa.job.jstorm.bolt.StgOrderInfoParserBolt
import com.di.mesa.job.jstorm.configure.VdianMqConfigure
import com.di.mesa.job.jstorm.spout.VdianMQSpout
import com.di.mesa.plugin.kudu.KuduConfigure
import com.di.mesa.plugin.kudu.storm.bolt.KuduBolt
import com.di.mesa.plugin.storm.CommonConfiure
import org.slf4j.{Logger, LoggerFactory}


/**
  * Created by Davi on 17/8/21.
  */
private[mesa] class StgOrderInfoEtlTopology extends MesaBaseTopology {

  @transient lazy private val LOG: Logger = LoggerFactory.getLogger(getClass)
  private var vmqSpoutParallelism: Int = 2
  private var kuduBoltParallelism: Int = 2
  private var stgOrderInfoParserBoltParallelism: Int = 2

  @throws(classOf[Exception])
  override def prepareTopologyConfig(args: Array[String]): Unit = {
    super.prepareTopologyConfig(args)

    // cluster config
    workers = 2

    this.config.put(CommonConfiure.SHOULD_RECORD_METRIC_TO_OPENTSDB, "false")
    this.config.put(CommonConfiure.OPENTSDB_URL, "http://10.8.96.120:4242,http://10.8.96.121:4242,http://10.8.96.122:4242")


    //enable zk notifier
    this.config.enableZkNotifer(false)

    //prepare vdianmq config
    this.config.put(VdianMqConfigure.VDIAN_MQ_SUBSCRIBED_TOPIC_TAGS, VdianMqConfigure.MQ_DEFAULT_TAG)
    this.config.put(VdianMqConfigure.VDIAN_MQ_SUBSCRIBED_TOPIC_NAME, VdianMqConfigure.MQ_TOPIC_ORDER_ALL_STATUS)
    this.config.put(VdianMqConfigure.VDIAN_MQ_SUBSCRIBED_ZK_ADDRESS, VdianMqConfigure.VMQ_ONLINE_ADDRESS)
    this.config.put(VdianMqConfigure.VDIAN_MQ_SUBSCRIBED_TOPIC_GROUPNAME, "di-mesa-order-to-kudu_group")

    //prepare kudu config
    this.config.put(KuduConfigure.KUDU_TABLE_NAME, "kudu_mesa.stg_order_info")
    this.config.put(KuduConfigure.KUDU_MASTER_ADDRESS, "10.1.7.111,10.1.7.112")
    this.config.put(KuduConfigure.KUDU_OPERATION_PROP, KuduConfigure.KUDU_OPERATION_INSERT)
    this.config.put(KuduConfigure.KUDU_BOLT_PRODUCER, "com.weidian.di.storm.mesa.storm.kudu.StgOrderInfoOperProducer")

    if (config.isLocalMode) {
      workers = 1
      vmqSpoutParallelism = 1
      kuduBoltParallelism = 1
      stgOrderInfoParserBoltParallelism = 1

      this.config.put(VdianMqConfigure.VDIAN_MQ_SUBSCRIBED_ZK_ADDRESS, VdianMqConfigure.VMQ_DAILY_ADDRESS)
      this.config.put(VdianMqConfigure.VDIAN_MQ_SUBSCRIBED_TOPIC_GROUPNAME, "di-mesa-order-to-kudu-test")
    }
  }


  @throws(classOf[Exception])
  override def prepareTopologyBuilder(): Unit = {
    super.prepareTopologyBuilder

    //config tick spout
    topologyBuilder.setSpout(CommonConfiure.TICK_SPOUT_NAME, getTickSpout, 1)

    topologyBuilder.setSpout(classOf[VdianMQSpout].getSimpleName, getVdianMQSpout, vmqSpoutParallelism)


    // config order info parser
    topologyBuilder.setBolt(classOf[StgOrderInfoParserBolt].getSimpleName, new StgOrderInfoParserBolt, stgOrderInfoParserBoltParallelism)
      .shuffleGrouping(classOf[VdianMQSpout].getSimpleName, VdianMqConfigure.VDIANMQ)
      .allGrouping(CommonConfiure.TICK_SPOUT_NAME, "count")

    //config sink
    topologyBuilder.setBolt(classOf[KuduBolt].getSimpleName, new KuduBolt, kuduBoltParallelism)
      .fieldsGrouping(classOf[StgOrderInfoParserBolt].getSimpleName, new Fields("seller_id", "buyer_id", "order_id"))
      .allGrouping(CommonConfiure.TICK_SPOUT_NAME, "count")
  }

}


object StgOrderInfoEtlTopology {
  def main(args: Array[String]): Unit = {
    new ToolRunner().run(new StgOrderInfoEtlTopology, args);
  }
}

