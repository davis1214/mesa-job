package com.di.mesa.sparkjob.util

import java.sql.{Connection, DriverManager, ResultSet}

import com.di.mesa.sparkjob.config.Config


/**
  * Created by Administrator on 17/7/3.
  */
object MysqlPersistTool {

  def mysqlPersist: (Iterator[(String, Long)]) => Unit = {
    part => {
      Class.forName("com.mysql.jdbc.Driver")
      var conn: Connection = DriverManager.getConnection(Config.getJdbcUrl)
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
