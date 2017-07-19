package com.di.mesa.streamingjob.monitor.common

object OperTypeEnums extends Enumeration {

  type OperTypeEnums = Value

  //1:Submit、2:Query、3:Kill
  val CREATE = Value("0")
  val SUBMIT = Value("1")
  val QUERY = Value("2")
  val KILL = Value("3")

  def getValueByOperTypeEnumsId(id: Int) = OperTypeEnums(id).toString()
  def getValueByOperTypeEnumsIns(ins: OperTypeEnums) = OperTypeEnums(ins.id).toString()

}
