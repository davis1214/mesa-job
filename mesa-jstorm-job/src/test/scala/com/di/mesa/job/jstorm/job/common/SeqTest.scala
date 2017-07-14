package com.di.mesa.job.jstorm.job.common

import org.scalatest.FunSuite

import scala.concurrent.duration._

/**
  * Created by Administrator on 17/7/11.
  */

class SeqTest

  extends FunSuite {


  private val defaultTopic = "user_action"

  test("test seq") {
    val args = Array("topic", "broker_list")


    args.drop(2)

    println(args.toString)

    //    val Seq(topic, brokers, _*) = Seq(defaultTopic, "localhost:9092") zip (0 to 1).map( case (defautl, n) => {
    //      args.maybe(m) getOrElse
    //    })


    assert(true)

  }


  def main(args: Array[String]) {

    val args = Array("topic", "broker_list", "zk_host")



    val topologyArgs = args.drop(2)


    topologyArgs.foreach(println)


    //    val Seq(topic, brokers, _*) = Seq(defaultTopic, "localhost:9092") zip (0 to 1) map { case (default, n) =>
    //      args.maybe(n) getOrElse default
    //    }


  }

}
