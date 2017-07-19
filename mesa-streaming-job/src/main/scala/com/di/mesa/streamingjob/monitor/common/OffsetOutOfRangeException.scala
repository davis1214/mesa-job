package com.di.mesa.streamingjob.monitor.common

class OffsetOutOfRangeException(message: String) extends RuntimeException(message) {
  def this() = this(null)
}
