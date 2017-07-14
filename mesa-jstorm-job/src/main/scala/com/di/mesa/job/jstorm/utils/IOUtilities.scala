package com.di.mesa.job.jstorm.utils

import scala.language.reflectiveCalls

/**
  * Input/Output Utilities
  *
  * @author lawrence.daniels@gmail.com
  */
object IOUtilities {

  type Closeable = {def close(): Unit}

  /**
    * Auto-close Extension
    *
    * @param closeable the given [[Closeable closeable]]
    */
  implicit class AutoCloseExtension[T <: Closeable](val closeable: T) extends AnyVal {

    @inline
    def use[S](block: T => S): S = try block(closeable) finally closeable.close()

  }

}
