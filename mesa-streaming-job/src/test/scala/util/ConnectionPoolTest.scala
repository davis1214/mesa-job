package util

import com.di.mesa.streamingjob.monitor.job.RtJob

object ConnectionPoolTest extends App with RtJob {

  //  val rtJobBean = getRtRuleByJobId("id_00001")
  //
  //  rtJobBean match {
  //    case Some(RtJobBean(ruleItem, ruleModule, ruleFilterFontains, ruleFilterRange, ruleKeyPattern, ruleMeasureType, ruleMeasureIndex)) => println("-->" + ruleKeyPattern)
  //	  //case Some(RtJobBean(ruleItem: String, ruleModule: String, ruleFilterFontains: String, ruleFilterRange: String, ruleKeyPattern: String, ruleMeasureType: String, ruleMeasureIndex: String)) => println("-->" + ruleKeyPattern)
  //    case None => println("find no rt-rule")
  //  }

  //getRtRuleByJobId("id_00001")

  updateAppIdByJobId("id_00001", "applid_test")
  println(lst)

  def test(): Option[String] = {
    val judge = true

    if (judge)
      Some("test")
    else None
  }

  val greeting: Option[String] = Some("Hello world")
  println(greeting.getOrElse("greetings"))

  val greeting2: Option[String] = None
  println(greeting2.getOrElse("greetings2"))

  case class User(
    id: Int,
    firstName: String,
    lastName: String,
    age: Int,
    gender: Option[String])

  object UserRepository {
    private val users = Map(1 -> User(1, "John", "Doe", 32, Some("male")),
      2 -> User(2, "Johanna", "Doe", 30, None))
    def findById(id: Int): Option[User] = users.get(id)
    def findAll = users.values
  }

  val user1 = UserRepository.findById(2)
  if (user1.isDefined) {
    println("user:" + user1.get.firstName)
  }

}