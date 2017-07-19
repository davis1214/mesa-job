package com.di.mesa.streamingjob.monitor.processor

import com.di.mesa.streamingjob.monitor.common.DatabaseException
import com.di.mesa.streamingjob.monitor.util.{ConnectionPool, SplitUtil}
import org.apache.spark.{Accumulator, Logging}
import org.apache.spark.rdd.RDD
import java.sql.{DriverManager, ResultSet, Connection}


/**
  * Created by davihe on 2016/3/8.
  */
object UdcLogProcessor extends Logging {


  //TODO 存储到制定的地方
  //TODO broadcast方式传递filter
  //TODO storageType:String , filter:String
  def process(filterList: List[Map[String, String]], rdd: RDD[(String, String)], lineAccumulator: Accumulator[Long]): Unit = {

   
    filterList.foreach(filterMap => {

      val notNullFilter = { a: String => !a.equals("") }
      val filterRdd = rdd.map {
        case (k, v) => {
          v
        }
      }.map(log => {
        lineAccumulator += 1
        logFilter(filterMap, log)
      }).filter(notNullFilter)

      val kvRdd = filterRdd.map(idvisitor => {
        if (idvisitor.indexOf("=>") == -1) {
          (idvisitor, 1l)
        } else {
          //含有加和计算的情况
          val key = idvisitor.split("=>")(0) + "=>" + idvisitor.split("=>")(1).split("&&")(0)
          val value = idvisitor.split("=>")(1).split("&&")(1).toLong
          (key, value)
        }
      })

      //kvRdd[DStream].updateStateByKey[Int](updateFunc)
      val logCountRdd = kvRdd.reduceByKey(_ + _)

      try {
        logCountRdd.foreachPartition(part => {

          //val conn_str = "jdbc:mysql://localhost:3306/test?user=root&password=root"
          //val conn = ConnectionPool.getConnection.getOrElse(null)

          Class.forName("com.mysql.jdbc.Driver")
          var conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/test?user=root&password=root")
          var statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)

          var ps = conn.prepareStatement("INSERT INTO udc_log_storm_test(value,createTime,keytype) VALUES (? ,? ,?) ")
          //val sql = "INSERT INTO udc_log_storm_test(value,createTime,keytype) VALUES (? ,? ,?) "
          part.foreach {
            case (k, v) => {

              val c = (System.currentTimeMillis() / 1000 - 60).toLong
              val sqlScript = "INSERT INTO udc_log_storm_test(value,createTime,keytype) VALUES (" + v + ", " + c + ", " + k + ") "
              println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>UdcLogMysql:" + sqlScript)

              //insertIntoMySQL(conn, sql, v, c, k)
              ps.setLong(1, v)
              ps.setLong(2, c)
              ps.setString(3, k)
              ps.executeUpdate()
            }
          }
          ps.close()
          conn.close()
          //ConnectionPool.closeConnection(conn)
        })
      } catch {
        case e: Exception => throw new DatabaseException("database error")
        case _ => logError("other exception")
      }

      def insertIntoMySQL(conn: Connection, sql: String, v: Long, c: Long, k: String): Unit = {
        try {
          val ps = conn.prepareStatement(sql)
          ps.setLong(1, v)
          ps.setLong(2, c)
          ps.setString(3, k)
          ps.executeUpdate()
          ps.close()

        } catch {
          case exception: Exception =>
            logError("Error in execution of query " + exception.getMessage + "\n-----------------------\n" + exception.printStackTrace() + "\n-----------------------------")

        }

      }

      //TODO write to Mysql
      //TODO add state-update

    })
  }

  val updateFunc = (currentValues: Seq[Int], preValue: Option[Int]) => {
    val curr = currentValues.sum
    val pre = preValue.getOrElse(0)
    Some(curr + pre)
  }

  def logFilter(filterMap: Map[String, String], line: String): String = {
    val resultlist = scala.collection.mutable.ListBuffer.empty[String];
    var errtag = true

    //TODO 转换成新的规则模型
    filterMap.foreach(e => {
      val tag = e._1 match {
        case "index" => indxefac(e._2, line)
        case "output" => outputfac(e._2, line)
        case "pattern" => patternfac(e._2, line)
        case "calcadd" => calcaddfac(e._2, line)
      }

      if (tag == false || tag == "") {
        errtag = false
      }
      if (tag.isInstanceOf[String]) {
        resultlist += tag.toString
      }
    })

    println(">>>>>>>>>>>>>>>>>UdcLogBolt:" + resultlist.toList.mkString("->"))
    //满足时输出 resultlist如果为空,返回未空
    resultlist.toList.mkString("->") //apiUrl
  }

  def indxefac(str: String, line: String): Boolean = {
    str.split("&&").foreach(one => {
      if (line.indexOf(one) == -1) {
        return false
      }
    })
    true
  }

  def outputfac(str: String, line: String): String = {
    str.split("&&").foreach(one => {
      if (line.indexOf(one) == -1) {
        return ""
      }
    })
    str.replace("&&", "->")
  }

  def patternfac(str: String, line: String): String = {
    val resultlist = scala.collection.mutable.ListBuffer.empty[String]
    str.split("&&").foreach(one => {
      val urltmp = one.r findFirstIn line match {
        case Some(one.r(url)) => url
        case None => ""
      }
      if (urltmp == "") {
        return ""
      } else {
        resultlist += urltmp.toString
      }
    })
    resultlist.toList.mkString("->")
  }

  def calcaddfac(str: String, line: String): String = {
    //key&&pattern
    val resultlist = scala.collection.mutable.ListBuffer.empty[String]
    val one = str.split("&&")(1)
    val urltmp = one.r findFirstIn line match {
      case Some(one.r(url)) => url
      case None => ""
    }
    resultlist += "=>" + str.split("&&")(0)
    if (urltmp == "") {
      return ""
    } else {
      resultlist += urltmp.toString
    }
    resultlist.toList.mkString("&&")
  }

}
