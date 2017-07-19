package util

import org.json4s._
import scala.collection.mutable.ArrayBuffer
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.write

object JsonUtil extends App {
  implicit val formats = Serialization.formats(NoTypeHints)
  val tagMap = Map("topic" -> "topicc", "module" -> "modelname")
 
  val c = "metic001"
  //val json = ("metric" -> c) ~ ("timestamp" -> "time0001") ~ ("value" -> "value-0001") ~ ("tags" -> tagMap)

  //println(write(json))

}