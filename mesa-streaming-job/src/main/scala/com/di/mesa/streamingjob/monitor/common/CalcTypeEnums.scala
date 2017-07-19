package com.di.mesa.streamingjob.monitor.common

object CalcTypeEnums extends Enumeration {

  type CalcTypeEnums = Value

  //Sum、Count、Avg、TopN，distinct

  val SUM = Value("SUM")
  val COUNT = Value("COUNT")
  val AVG = Value("AVG")
  val DISTINCT = Value("DISTINCT")
  val TOPN = Value("TopN")

  def getValueByCalcTypeEnumsId(id: Int) = CalcTypeEnums(id).toString()
  def getValueByCalcTypeEnumsIns(ins: CalcTypeEnums) = CalcTypeEnums(ins.id).toString()

}
