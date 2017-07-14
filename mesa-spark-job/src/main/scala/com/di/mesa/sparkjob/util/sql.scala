package com.di.mesa.sparkjob.util

/**
  * Created by time on 17/2/9.
  */

import java.sql.DriverManager
import com.di.mesa.sparkjob.config.Config._

object sql {
  //var connection: Connection = null
  //注册Driver
  Class.forName(driver)
  //得到连接
  val connection = DriverManager.getConnection(url, username, password)
  val statement = connection.createStatement


  def update(str: String) = {
    try {
      val num = statement.executeUpdate(str)
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }

  def insertIM() = {
    """insert into IM(uid, time, type, msg) values('%s', %d, %s, "%s") """
  }

  def close() = {
    //关闭连接，释放资源
    connection.close()
  }
}
