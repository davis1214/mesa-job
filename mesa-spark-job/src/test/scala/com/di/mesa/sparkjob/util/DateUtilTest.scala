package com.di.mesa.sparkjob.util

import java.sql._


/**
  *
  * Created by Administrator on 17/7/7.
  */
object DateUtilTest {


  def main(args: scala.Array[String]) {
    val ss :Long = DateUtil.getlastDay.getMillis
    println(ss)

    println(new Date(ss))

  }
}
