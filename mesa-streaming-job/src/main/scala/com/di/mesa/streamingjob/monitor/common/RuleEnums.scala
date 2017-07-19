package com.di.mesa.streamingjob.monitor.common

/**
 * Created by Administrator on 16/6/6.
 */
object RuleEnums extends Enumeration {

  type RuleEnums = Value

  val RULE_MODULE = Value("Rule_Module")
  val RULE_ITEM = Value("Rule_Item")
  val RULE_FILTER_CONTAINS = Value("Rule_Filter_Contains")
  val RULE_FILTER_RANGE = Value("Rule_Filter_Range")
  val RULE_KEY_PATTERN = Value("Rule_Key_Pattern")
  val RULE_MEASURE_TYPE = Value("Rule_Measure_Type")
  val RULE_MEASURE_INDEX = Value("Rule_Measure_Index")

  def getValueByRuleEnumsId(id: Int) = RuleEnums(id).toString()
  def getValueByRuleEnumsIns(ins: RuleEnums) = RuleEnums(ins.id).toString()

}
