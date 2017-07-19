package com.di.mesa.streamingjob

import com.di.mesa.streamingjob.monitor.common.RuleEnums._

//object TrafficLightColor extends Enumeration {
//    val Red, Yellow, Green = Value
//}

//object TrafficLightColor extends Enumeration {
//  val Red = Value(0, "Stop") // Red.toString() to get "Stop"
//  val Yellow = Value(10) // Name "Yellow" 
//  val Green = Value("Go") // ID 11
//}

object TrafficLightColor extends Enumeration {
  type TrafficLightColor = Value
  val Red = Value(0, "Stop")
  val Yellow = Value(10)
  val Green = Value("Go")
}

object Run extends App {
  import TrafficLightColor._
  
  def doWhat(color: TrafficLightColor) = {
    if (color == Red) "stop"
    else if (color == Yellow) "hurry up" else "go"
  }

  println("----->" + TrafficLightColor(TrafficLightColor.Green.id))
  // load Red
  println("--->" + TrafficLightColor(0)) // Calls Enumeration.apply 
  println("--->" + TrafficLightColor.withName("Go"))
  
  
  
  println("value:" + getValueByRuleEnumsId(RULE_ITEM.id))
  
}