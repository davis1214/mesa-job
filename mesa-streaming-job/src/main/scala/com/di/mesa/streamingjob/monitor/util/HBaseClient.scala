package com.di.mesa.streamingjob.monitor.util

import org.apache.hadoop.conf.Configuration
//import org.apache.hadoop.hbase.client.{HConnectionManager, HConnection}

/**
 * Created by davihe on 2016/3/7.
 */
object HBaseClient extends Serializable{
//  @transient private var connection:HConnection =null
//  def getConnection(conf: Configuration) : HConnection ={
//    if(connection == null){
//      connection=HConnectionManager.createConnection(conf)
//      val hook=new Thread{
//        override def run=connection.close()
//      }
//      sys.addShutdownHook(hook.run)
//    }
//    connection
//  }
}
