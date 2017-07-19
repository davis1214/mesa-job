package com.di.mesa.streamingjob

import com.di.mesa.streamingjob.monitor.processor.PublicLogProcessor

object FilterFuncTests extends App {

  val line = scala.io.Source.fromFile("/Users/Administrator/Documents/test/source/app_udc_account_01.COMPLETED").getLines()

  var lst = List(Map("Rule_Filter_Contains" -> "NOTICE", "Rule_Key_Pattern" -> """url\[([^(\?|\])]+).*?\]""", "Rule_Measure_Type" -> "SUM"))

  val filterMap = Map("Rule_Filter_Contains" -> "NOTICE", "Rule_Key_Pattern" -> """url\[([^(\?|\])]+).*?\]""", "Rule_Measure_Type" -> "SUM")


  //PublicLogProcessor.logFilter(filterMap, line)

  //line.foreach { f => println("-->" + f) }

  val aa = line.filter { x => PublicLogProcessor.logFilter(filterMap, x) }

  val bb = aa.map { x => PublicLogProcessor.logProcess(filterMap, x) }


  //aa.foreach { println }

  println("\n\n")
  aa.foreach { x => println("------->result:" + x) }

  println("end")

}