package com.di.mesa.job.jstorm.configure

/**
  * Created by Administrator on 17/8/3.
  */
object MesaConfigure {

  /**
    * running status
    */
  val _SUCCESS: Int = 1
  val _ERROR: Int = -1
  val _EXIT: Int = 0


  /**
    * configuration of topology (common)
    */
//  val TOPOLOGY_NAME: String = "topology.name"
//  val RUNNING_MODE: String = "topology.running.type"
  val RUNNING_MODE_LOCAL: String = "local"
  val RUNNING_MODE_CLUSTER: String = "cluster"
  val SUBSCRIBED_TOPIC_NAME: String = "subscribe.topic.name"
  val SUBSCRIBED_TOPIC_GROUP_NAME: String = "subscribe.topic.group.name"


  val VDIAN_MQ_ADDRESS: String = "busi.vdianmq.address"

  val TICK_SPOUT_NAME: String = "TICK_SPOUT"


}
