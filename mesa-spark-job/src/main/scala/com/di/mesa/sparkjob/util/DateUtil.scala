package com.di.mesa.sparkjob.util

import java.util.Calendar

import org.joda.time.DateTime


/**
  * Created by Administrator on 17/7/4.
  */
object DateUtil extends Serializable{

  def getDayOfMonth: Int = Calendar.getInstance().get(Calendar.DAY_OF_MONTH);

  def getLastDayOfMonth: Int = Calendar.getInstance().get(Calendar.DAY_OF_MONTH) - 1;

  def getlastDayFmt: String = {
    val datetime = new DateTime()
    val yesterday = datetime.plusDays(-1);
    yesterday.toString("yyyy-MM-dd")
  }

  def getlastDay: DateTime = {
    val datetime = new DateTime()
    datetime.plusDays(-1);
  }

}
