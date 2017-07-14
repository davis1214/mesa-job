package com.di.mesa.sparkjob.scripts

import com.di.mesa.sparkjob.util.{DateUtil, SparkCommon}
import org.apache.spark.rdd.RDD

/**
  * Created by Administrator on 17/7/9.
  */
object Test {

  implicit def string2boolean(arg: String): Boolean = arg.toBoolean


  def main(args: scala.Array[String]) {

    val sourceRdd: RDD[String] = initSourceRdd(true, true)

    val parserRdd = sourceRdd.map(f => {

      println("-->" + f)
      if (f.startsWith("1") || f.startsWith("2")) {
        (f, 1)
      }

    })

    parserRdd.collect()

  }

  def initSourceRdd(isLocalMode: Boolean, isTest: Boolean): RDD[String] = {

    val sparkContext = if (isLocalMode) SparkCommon.getLocalSparkContext
    else SparkCommon.getClusterSparkContext


    if (isLocalMode) {

      val lines = scala.Array("111", "2222", "3333")

      sparkContext.parallelize(lines)
    } else {

      val lastDayFmt = DateUtil.getlastDayFmt

      val file = if (isTest) s"/data/rawlog/autoCollect/www_ynflmrx9e52e701c527e_vshop/${lastDayFmt}/07/*"
      else s"/data/rawlog/autoCollect/www_ynflmrx9e52e701c527e_vshop/${lastDayFmt}/*/*"

      sparkContext.textFile(file)
    }

  }


}
