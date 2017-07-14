package com.di.mesa.sparkjob.util

/**
  * Created by Administrator on 17/7/3.
  */
object StringUtil {

  def isEmpty(v: String): Boolean = Option(v) match {
    case Some(s: String) => true
    case None => false
  }


  def main(args: Array[String]) {

    val b = "ss"

    println(isEmpty(b))

  }

}
