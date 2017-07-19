package com.di.mesa.streamingjob.monitor.util

import org.apache.hadoop.conf.Configuration
//import org.apache.hadoop.hbase.HConstants

/**
 * Created by davihe on 2016/3/2.
 */
object HBaseUtil {

//  def createHBaseConfMap:collection.mutable.Map[String,String]={
//    val cf=collection.mutable.Map[String,String]()
//    cf.put(HConstants.HBASE_CLIENT_PAUSE, "3000")
//    cf.put(HConstants.HBASE_CLIENT_RETRIES_NUMBER, "5")
//    cf.put(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, "60000")
//
//    cf.put(HConstants.ZOOKEEPER_QUORUM,"dn074021.heracles.sohuno.com,kb.heracles.sohuno.com,monitor.heracles.sohuno.com")
//
//    cf.put("scala.hbase.master.keytab.file","/etc/security/keytabs/scala.hbase.service.keytab")
//    cf.put("scala.hbase.master.kerberos.principal", "scala/hbase/_HOST@HERACLES.SOHUNO.COM")
//    cf.put("scala.hbase.master.info.bindAddress","0.0.0.0")
//    cf.put("scala.hbase.master.info.port","60010")
//
//    cf.put("scala.hbase.regionserver.keytab.file","/etc/security/keytabs/scala.hbase.service.keytab")
//    cf.put("scala.hbase.regionserver.kerberos.principal", "scala/hbase/_HOST@HERACLES.SOHUNO.COM")
//    cf.put("scala.hbase.regionserver.info.port","60030")
//
//    cf.put("zookeeper.znode.parent","/scala.hbase-secure")
//    cf.put("scala.hbase.security.authentication","kerberos")
//    cf.put("scala.hbase.security.authorization","true")
//    cf.put("scala.hbase.coprocessor.region.classes","org.apache.hadoop.scala.hbase.security.token.TokenProvider,org.apache.hadoop.scala.hbase.security.access.SecureBulkLoadEndpoint,org.apache.hadoop.scala.hbase.security.access.AccessController")
//
//    cf.put("scala.hbase.tmp.dir","/opt/hadoop/scala.hbase")
//    cf.put("scala.hbase.rootdir","hdfs://heracles/apps/scala.hbase/data")
//    cf.put("scala.hbase.superuser", "scala/hbase")
//    cf.put("scala.hbase.zookeeper.property.clientPort","2181")
//    cf.put("scala.hbase.cluster.distributed","true")
//    cf
//  }
//
//  def setHBaseConfig(cf: Configuration): Unit ={
//    createHBaseConfMap.map{
//      case(k,v)=>{
//        cf.set(k,v)
//      }
//    }
//  }
}
