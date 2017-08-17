package com.di.mesa.job.jstorm.topology

import com.di.mesa.plugin.storm.MesaConfig

/**
  * Created by davi on 17/8/3.
  */
trait Tool {

  var config: MesaConfig = new MesaConfig

  @throws(classOf[Exception])
  def run(args: Array[String])

  def getConf: MesaConfig = {
    return config
  }

  def setConf(config: MesaConfig) {
    this.config = config
  }

}
