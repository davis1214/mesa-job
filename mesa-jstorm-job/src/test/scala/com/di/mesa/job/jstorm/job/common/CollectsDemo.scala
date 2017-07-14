package com.di.mesa.job.jstorm.job.common

/**
  * Created by Administrator on 17/7/11.
  */
object CollectsDemo extends App {


  val arg = Array("topic", "broker_list", "zk_host")


  arg.dropRight(4).foreach(println)

  println("--<>")

  val topologyArgs = arg.drop(2)

  topologyArgs.foreach(println)


  println("--<>")

  arg.foreach(println)


}
