package util

object StringGenerator extends App {

  val str = "rule_item,rule_module,rule_filter_contains,rule_filter_range,rule_key_pattern,rule_measure_type,rule_measure_index"

  str.split(",").foreach { x =>
    {
      
      x.split("_")

    }
  }
  
  //ruleItem,ruleModule,ruleFilterFontains,ruleFilterRange,ruleKeyPattern,ruleMeasureType,ruleMeasureIndex

}