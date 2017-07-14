package com.di.mesa.sparkjob.util

import org.joda.time.format.DateTimeFormat
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

object JsonUtilTest {

  implicit val jsonFormatter = org.json4s.DefaultFormats
  private val dateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss SSS")


  def main(args: Array[String]): Unit = {
    val a = parse(""" { "numbers" : [1, 2, 3, 4] } """)
    println(a.toString)
    val b = parse("""{"name":"Toy","price":35.35}""", useBigDecimalForDouble = true)
    println(b.toString)

    val c = List(1, 2, 3)
    val d = compact(render(c))
    println(d)
    val e = ("name" -> "joe")
    val f = compact((render(e)))
    println(f)
    val g = ("name" -> "joe") ~ ("age" -> 35)
    val h = compact(render(g))
    println(h)
    val i = ("name" -> "joe") ~ ("age" -> Some(35))
    val j = compact(render(i))
    println(j)
    val k = ("name" -> "joe") ~ ("age" -> (None: Option[Int]))
    val l = compact(render(k))
    println(l)

    //定义json
    println("===========================")
    //推荐这种方式，因为可以用在使用map
    val jsonobj = (
      ("name" -> "xiaoming") ~
        ("age" -> 12)
      )
    println(jsonobj)
    println(compact(render(jsonobj)))

    val jsonobjp = parse(
      """{
            "name":"xiaogang",
            "age":12
          }""")
    println(jsonobjp)
    println(compact(render(jsonobjp)))

    //通过类生成json
    println("===========================")
    case class Winner(id: Long, numbers: List[Int])

    case class Lotto(id: Long, winningNumbers: List[Int], winners: List[Winner], drawDate: Option[java.util.Date])
    val winners = List(Winner(23, List(2, 45, 34, 23, 3, 5)), Winner(54, List(52, 3, 12, 11, 18, 22)))

    val lotto = Lotto(5, List(2, 45, 34, 23, 7, 5, 3), winners, None)

    val json =
      ("lotto" ->
        ("lotto-id" -> lotto.id) ~
          ("winning-numbers" -> lotto.winningNumbers) ~
          ("draw-date" -> lotto.drawDate.map(_.toString)) ~
          ("winners" ->
            lotto.winners.map { w =>
              (("winner-id" -> w.id) ~
                ("numbers" -> w.numbers))
            }))

    println(compact(render(json)))

    println("-->" + jsonFormatter.formatted(compact(render(json))))

    val aa = dateTimeFormatter.parseDateTime("2017-09-07 11:11:32 342")

    println(aa)
  }
}

