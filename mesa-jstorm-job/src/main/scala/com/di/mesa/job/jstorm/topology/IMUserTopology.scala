package com.di.mesa.job.jstorm.topology

import backtype.storm.spout.SchemeAsMultiScheme
import backtype.storm.topology.TopologyBuilder
import backtype.storm.tuple.Fields
import backtype.storm.utils.Utils
import backtype.storm.{Config, LocalCluster, StormSubmitter}
import com.di.mesa.job.jstorm.bolt.{IMMQBolt, IMParserBolt}
import com.di.mesa.job.jstorm.spout.SampleDataSpout
import storm.kafka._

import scala.collection.JavaConversions._

/**
  * 卖家最后活跃时间通知 IM
  */
object IMUserTopology {

  //implicit def string2Boolean(str: String): Boolean = str.toBoolean

  def main(args: Array[String]): Unit = {

    val Array(numworker, parserBoltParallism, mqBoltParallism) = Array(1, 1, 1)

    val sourceTopic = "topic1,topic2"

    val storm_zookeeper_servers: String = "localhost";
    val kafka_zookeeper_hosts = "localhost:2181";

    val conf = initTopologyConfig(args)
    val submitCluster = conf.get("op.running.model").toString
    val builder: TopologyBuilder = new TopologyBuilder();

    //BrokerHosts hosts, String topic, String zkRoot, String id
    if (!submitCluster.equals("Local")) {
      val hosts = new ZkHosts(kafka_zookeeper_hosts)
      val zkRoot: String = "/di/imtopology_test"
      val groupId = "test"

      sourceTopic.split(",").foreach { topic: String => {
        val spoutId: String = classOf[IMUserTopology].getName.concat("_").concat(topic)

        val spoutConfig: SpoutConfig = new SpoutConfig(hosts, topic, zkRoot, groupId);
        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        spoutConfig.zkServers = storm_zookeeper_servers.split(",").toList
        builder.setSpout("spout", new KafkaSpout(spoutConfig), 3);
      }
      }
    } else {
      val sourceId = sourceTopic.split(",")(0)
      builder.setSpout(sourceId, new SampleDataSpout(""), 1);
    }

    builder.setBolt(classOf[IMParserBolt].getName, new IMParserBolt(sourceTopic.split(",")(0)), parserBoltParallism)
      .shuffleGrouping("topic2")

    builder.setBolt(classOf[IMMQBolt].getName,
      new IMMQBolt(conf.get("op.zookeeper.address").toString, conf.get("busi.output.tag").toString), mqBoltParallism)
      .fieldsGrouping(classOf[IMParserBolt].getName, new Fields("userId"));


    if (!submitCluster.equals("Local")) {
      conf.setNumWorkers(numworker)
      StormSubmitter.submitTopology(conf.get("topology.name").toString, conf, builder.createTopology())
    } else {
      println("prepare to start local storm cluster !")


      try {
        val cluster: LocalCluster = new LocalCluster()
        cluster.submitTopology(classOf[IMUserTopology].getSimpleName, conf, builder.createTopology())
        Utils.sleep(50000000)
        cluster.shutdown()
      } catch {
        case e: Exception => e.printStackTrace()
        case _ => println("===> ambiguous ")
      }


      println("local storm cluster shut down !")

    }

  }


  def initTopologyConfig(args: Array[String]) = {

    val topologyName = if (args.length > 0) args(0) else classOf[IMUserTopology].getSimpleName
    val runningMode = if (args.length > 1) args(1) else "Local"
    val mqZkHosts = if (args.length > 2) args(2) else "zk1.com,zk2.com,zk3.com"

    val tag = "userActiveTime"
    val sinkTopic = "user_action"
    val sinkMQ = "vmq"

    val conf = new Config();
    conf.setDebug(false);
    conf.put("topology.name", topologyName);
    conf.put("busi.output.tag", tag);
    conf.put("busi.output.mq", sinkMQ)
    conf.put("busi.output.topic", sinkTopic);

    conf.put("op.running.model", runningMode);
    conf.put("op.zookeeper.address", mqZkHosts);

    conf.setMaxSpoutPending(1000000);
    conf.setMessageTimeoutSecs(90);

    conf
  }


}

class IMUserTopology(topic: String, zk: String) {
}
