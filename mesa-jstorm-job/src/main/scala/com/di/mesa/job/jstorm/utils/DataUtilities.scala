package com.di.mesa.job.jstorm.utils

/**
  * Created by Administrator on 17/7/12.
  */

import java.nio.ByteBuffer
import java.util

import scala.concurrent.duration.Duration
import scala.language.implicitConversions

/**
  * Data Utilities
  *
  * @author lawrence.daniels@gmail.com
  */
object DataUtilities {

  /**
    * Implicit conversion from durations to milliseconds
    *
    * @param d the given [[Duration duration]]
    * @return the duration as milliseconds
    */
  implicit def durationToMillis(d: Duration): Long = d.toMillis

  /**
    * Array Extensions
    *
    * @param array the given [[Array array]]
    */
  implicit class ArrayExtensions[T](val array: Array[T]) extends AnyVal {

    @inline
    def maybe(index: Int) = if (array.length > index) Some(array(index)) else None

  }

  /**
    * UUID Extensions
    *
    * @param uuid the given [[util.UUID UUID]]
    */
  implicit class UUIDExtensions(val uuid: util.UUID) extends AnyVal {

    @inline
    def toByteArray = {
      val bb = ByteBuffer.wrap(new Array[Byte](16))
      bb.putLong(uuid.getMostSignificantBits)
      bb.putLong(uuid.getLeastSignificantBits)
      bb.array()
    }

  }

}