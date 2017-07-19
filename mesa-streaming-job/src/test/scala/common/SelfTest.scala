package common

class SelfTest {
  self =>
  val x = "2"

  def foo = self.x

}

object SelfTest {

  def main(args: Array[String]): Unit = {
    val selfTest = new SelfTest
    println(selfTest.foo)
  }

}
