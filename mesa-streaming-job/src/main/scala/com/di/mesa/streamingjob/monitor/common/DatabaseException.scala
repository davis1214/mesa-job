package com.di.mesa.streamingjob.monitor.common

/**
  * Created by Administrator on 16/6/1.
  */
class DatabaseException(message: String) extends RuntimeException(message) {
  def this() = this(null)
}
