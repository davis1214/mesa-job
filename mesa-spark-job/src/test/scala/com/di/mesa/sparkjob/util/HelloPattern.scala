package com.di.mesa.sparkjob.util

//模式类之间不能互相继承，必须统一继承一个抽象类或者trait
class DataFrameWork

//模式类，与普通类没什么区别，只是一定要有参数
case class ComputationFramework(name: String, popular: Boolean) extends DataFrameWork

case class StorageFramework(name: String, popular: Boolean) extends DataFrameWork

object HelloPattern {
  def main(args: Array[String]): Unit = {

    getSalary("dddd", 6)
    getMatchType(6.0)

    getMatchCollection(Array("Scala", "hadoop"))

    getBigDataType(ComputationFramework("Spark", true))


    val _map = Map("Spark" -> "hot", "hadoop" -> "half_hot")
    getValue("Spark", _map)

  }

  def getSalary(name: String, age: Int) {
    name match {
      //从前往后匹配
      case "Spark" => println("$150000/year")
      case "Hadoop" => println("$100000/year")
      //加入判断条件(用变量接受参数)
      case _name if age >= 5 => println(name + ":" + age + " $140000/year")
      case _ => println("$90000/year") //都不匹配时
    }
  }

  //对类型进行匹配
  def getMatchType(msg: Any) {
    msg match {
      case i: Int => println("Integer")
      case s: String => println("String")
      case d: Double => println("Double")
      case _ => println("Unknow type")
    }
  }

  //对数组的匹配
  def getMatchCollection(msg: Array[String]) {
    msg match {

      case Array("Scala") => println("one element")
      case Array("Scala", "Java") => println("two element")
      //以Scala开头的数组(可有多个成员)
      case Array("Scala", _*) => println("many element begin from scala")
      case _ => println("Unknow type")
    }
  }

  //对类的匹配
  def getBigDataType(data: DataFrameWork) {
    data match {
      case ComputationFramework(name, popular) =>
        println("computationFramework  " + "name:" + name + " popular:" + popular)
      case StorageFramework(name, popular) =>
        println("StorageFramework  " + "name:" + name + " popular:" + popular)
      case _ => println("Some other type")

    }
  }

  /**
    * 对map的匹配
    * 特别注意some和none的使用
    */
  def getValue(key: String, content: Map[String, String]) {
    content.get(key) match {
      case Some(value) => println(value)
      case None => println("No found!!")
    }
  }

}
