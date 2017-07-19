package com.di.mesa.streamingjob.monitor.util

import java.text.SimpleDateFormat
import java.util.Locale

/**
 * Created by davihe on 2016/3/7.
 */
object SplitUtil {

   val platMap=Map[String,String](
     "pc"->"10101",
     "ifox"->"10102",
     "1"->"20303",
     "3"->"30303",
     "6"->"30403",
     "0"->"20403",
     "h1"->"20304",
     "h3"->"30304",
     "h6"->"30404"
   )

   val SPLIT="\t"
   val COL_SPLIT="-"
   val ROW_KEY_SPLIT="|"
   val unixSdf=new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z",Locale.ENGLISH)
   val sdf=new SimpleDateFormat("yyyyMMddHHmm")

   def parseDate(s:String,format:String):String={
     val date=unixSdf.parse(s)
     val sdf_t=new SimpleDateFormat(format)
     sdf_t.format(date)
   }

  def parseDate(s:String):String={
    val date=unixSdf.parse(s)
    sdf.format(date)
  }

  def parseLogDate(s:String):String={
    parseDate(s.substring(1,s.length-1),"yyyy-MM-dd-HH-mm")
  }

  def parseLogDate(s:String,format:String):String={
    parseDate(s.substring(1,s.length-1),format)
  }

  def parseUrlParam(s:String):Map[String,String]={
   val map= s.split("&").map(a=>{
      val tmp=a.split("=",2)
      if(tmp.length==2){
        (tmp(0),tmp(1))
      }else{
        ("","")
      }
    }).toMap
    map
  }



}
