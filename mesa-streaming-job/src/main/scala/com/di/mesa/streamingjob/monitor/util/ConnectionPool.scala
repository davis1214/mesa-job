package com.di.mesa.streamingjob.monitor.util

/**
 * Created by Administrator on 16/5/30.
 */

import java.sql._

import com.jolbox.bonecp.{ BoneCP, BoneCPConfig }

object ConnectionPool {
  	val conn_str = "jdbc:mysql://localhost:3306/test?user=root&password=root"
  //val logger = LoggerFactory.getLogger(this.getClass)
  @transient private val connectionPool = {
    try {
      Class.forName("com.mysql.jdbc.Driver")

      val config = new BoneCPConfig()
//      config.setJdbcUrl("jdbc:mysql://localhost:3306/test")
//      config.setUsername("root")
//      config.setPassword("root")
      config.setJdbcUrl("jdbc:mysql://localhost:3306/logdata")
      config.setUsername("logdata_wb_w")
      config.setPassword("6VY66U4c4n1OhkfDV35R")
      config.setLazyInit(true)

      config.setMinConnectionsPerPartition(3)
      config.setMaxConnectionsPerPartition(5)
      config.setPartitionCount(5)
      config.setCloseConnectionWatch(true)
      config.setLogStatementsEnabled(false)

      Some(new BoneCP(config))
    } catch {
      case exception: Exception =>
        println("Error in creation of connection pool" + exception.printStackTrace())
        None
      case e: SQLException =>
        throw new RuntimeException("Can\'t register driver!");
    }
  }

  def getConnection: Option[Connection] = {
    connectionPool match {
      case Some(connPool) => Some(connPool.getConnection)
      case None           => None
    }
  }

  def closeConnection(connection: Connection): Unit = {
    if (!connection.isClosed) {
      connection.close()

    }
  }

}
