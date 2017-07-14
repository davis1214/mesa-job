package com.di.mesa.sparkjob.common

import java.sql.{Connection, DriverManager, ResultSet}

import org.json4s.JsonDSL._
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.scalatest.{FunSuite, ShouldMatchers}

import scala.collection.mutable.ArrayBuffer
import com.github.nscala_time.time.Imports._


/**
  * Created by Administrator on 17/7/4.
  */
class FunSuiteTest extends FunSuite {


  class Artist(val firstName: String, val lastName: String)

  class Album(val title: String, val year: Int, val artist: Artist)

  test("testGetClusterId") {
    assert(214751246815854592L === 214751246815854592L)
  }


  //差集
  test("Test difference") {
    val a = Set("a", "b", "a", "c")
    val b = Set("b", "d")
    assert(a -- b === Set("a", "c"))
  }

  //交集
  test("Test intersection") {
    val a = Set("a", "b", "a", "c")
    val b = Set("b", "d")
    assert(a.intersect(b) === Set("b"))
  }

  //并集
  test("Test union") {
    val a = Set("a", "b", "a", "c")
    val b = Set("b", "d")
    assert(a ++ b === Set("a", "b", "c", "d"))
  }


  implicit val jsonFormatter = org.json4s.DefaultFormats
  private val dateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss SSS")


  test("json build") {
    var metrics = ArrayBuffer[JsonAST.JValue]()
    metrics += ("1s" -> 1)
    metrics += ("50ms" -> 2)
    metrics += ("100ms" -> 3)
    metrics += ("200ms" -> 4)

    assert (compact(render(metrics)) === """[{"1s":1},{"50ms":2},{"100ms":3},{"200ms":4}]""")

  }

  test("create mysql table test") {

    val datestr = "1"

    def createTable: Unit = {

      Class.forName("com.mysql.jdbc.Driver")
      var conn: Connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/test?user=root&password=root")
      var statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)

      val table =
        s"""
           |create table if not exists `requset_time_info_$datestr` (
           |`id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT '自增id',
           |`url` varchar(64) DEFAULT '' COMMENT '接口url',
           |`mode` varchar(32) DEFAULT '' COMMENT '模块名',
           |`request_count` int(26) unsigned NOT NULL DEFAULT 0 COMMENT '请求总次数',
           |`time_count` varchar(512) DEFAULT '' COMMENT '超时信息JSON:50ms,100ms,150ms,200ms,500ms,1s',
           |`add_time` timestamp NOT NULL DEFAULT '1971-01-01 00:00:00' COMMENT '添加时间',
           |`update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
           |`extends` varchar(128) DEFAULT '' COMMENT '扩展字段',
           |PRIMARY KEY (`id`),
           |UNIQUE KEY `uniq_url` (`url`)
           |) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8 COMMENT='请求耗时统计表 王波 ${datestr}';
      """.stripMargin

      println(table)

      var ps = conn.prepareStatement(table)
      ps.execute()
      ps.close()

      conn.close()
    }

    def getTableCountNum: Int = {

      Class.forName("com.mysql.jdbc.Driver")
      var conn: Connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/test?user=root&password=root")
      var statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)


      var st = conn.prepareStatement(s"select count(*) as cnt from `requset_time_info_$datestr` where 1=0 ;")
      val rs = st.executeQuery()

      rs.next()

      val num = rs.getInt("cnt")

      println("cnt " + num)

      rs.close()
      st.close()
      conn.close()

      num
    }

    //createTable

    //assert(getTableCountNum === 0)


  }

}
