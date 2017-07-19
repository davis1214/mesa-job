package com.di.mesa.streamingjob.monitor.storage

import java.sql.{ Connection, DriverManager, ResultSet }
import com.sun.jersey.api.client.Client
import org.json4s._
import org.json4s.JsonDSL._
import scala.collection.mutable.ArrayBuffer
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.write
import com.di.mesa.streamingjob.monitor.common.OpentsClient
import javax.ws.rs.core.MediaType
import com.sun.jersey.api.client.ClientResponse

object RtStorage {

  def opentsPersist(topic: Option[String], module: Option[String]): (Iterator[(String, Long)]) => Unit = {
    part =>
      {
        implicit val formats = Serialization.formats(NoTypeHints)
        var metrics = ArrayBuffer[JsonAST.JObject]()
        part.foreach {
          case (k, v) => {
            val tagMap = Map("keytype" -> k, "module" -> module.get)
            val c = (System.currentTimeMillis() / 1000 - 60).toLong
            //val json = ("metric" -> k) ~ ("timestamp" -> c) ~ ("value" -> v) ~ ("tags" -> tagMap)
            val json = ("metric" -> topic.get) ~ ("timestamp" -> c) ~ ("value" -> v) ~ ("tags" -> tagMap)
            metrics += json
          }
        }

        val json = write(metrics.toList)
        println("-->" + json.toString())
        if (!json.isEmpty())
          println(OpentsClient.put(json))
      }
  }

  //TODO 封装函数,适应不同类型的存储介质
  def mysqlPersist: (Iterator[(String, Long)]) => Unit = {
    part =>
      {
        Class.forName("com.mysql.jdbc.Driver")
        var conn: Connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/test?user=root&password=root")
        var statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)

        var ps = conn.prepareStatement("INSERT INTO udc_log_storm_test(value,createTime,keytype) VALUES (? ,? ,?) ")
        part.foreach {
          case (k, v) => {
            val c = (System.currentTimeMillis() / 1000 - 60).toLong
            val sqlScript = "INSERT INTO udc_log_storm_test(value,createTime,keytype) VALUES (" + v + ", " + c + ", " + k + ") "
            println(">>>UdcLogMysql:" + sqlScript)

            ps.setLong(1, v)
            ps.setLong(2, c)
            ps.setString(3, k)
            ps.executeUpdate()
          }
        }
        ps.close()
        conn.close()
      }
  }

}