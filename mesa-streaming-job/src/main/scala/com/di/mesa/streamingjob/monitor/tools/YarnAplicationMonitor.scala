package com.di.mesa.streamingjob.monitor.tools

import org.apache.hadoop.security.{ SecurityUtil, UserGroupInformation }
import org.apache.hadoop.yarn.api.records.{ ApplicationId, YarnApplicationState }
import com.di.mesa.streamingjob.monitor.common.OperTypeEnums._
import com.di.mesa.streamingjob.monitor.common.OperTypeEnums

import org.apache.hadoop.yarn.exceptions.ApplicationNotFoundException

/**
 * Created by davihe on 2016/6/12.
 */
class YarnAplicationMonitor{
  
}

object YarnAplicationMonitor {

  case class ApplicationState(result: String, description: String)
//  def main(args: Array[String]) {
//    manageApplication(args)
//  }

  def manageApplication(args: Array[String]) :ApplicationState = {
    val conf = new org.apache.hadoop.conf.Configuration()
    val yarnClient = org.apache.hadoop.yarn.client.api.YarnClient.createYarnClient()
    yarnClient.init(conf)

    //val oper = "1" // 1:Submit、2:Query、3:Kill
    val Array(topic, applicationIdstr, oper) = args
    val Array(timestame, appId) = applicationIdstr.replace(ApplicationId.appIdStrPrefix, "").split("_", 2)

    println(s"application info timestame:${timestame},appId:${appId}")

    yarnClient.start()
    val applicatioId = ApplicationId.newInstance(timestame.toLong, appId.toInt)

    def killApplication(): ApplicationState = {
      yarnClient.killApplication(applicatioId);
      ApplicationState("Successed", s"${applicationIdstr} killed")
    }

    def queryApplication(): ApplicationState = {
      val stateReport = yarnClient.getApplicationReport(applicatioId)

      def getFormattedTime(timestamp: Long): String =
        org.apache.commons.lang3.time.DateFormatUtils.format(timestamp, "yyyy-MM-dd HH:mm:ss")

      def buildStatReportMsg(): ApplicationState = { //TODO 停止一个任务 提交任务
        val now = getFormattedTime(System.currentTimeMillis())
        val startTime = getFormattedTime(stateReport.getStartTime)
        //val msg = s"${now}-Topic:${topic},ApplicationState:${stateReport.getYarnApplicationState.toString},Progress:${stateReport.getProgress} FinalApplicationStatus:${stateReport.getFinalApplicationStatus.toString},StartTime:${startTime}"

        val msg = s"ApplicationState:${stateReport.getYarnApplicationState.toString},Progress:${stateReport.getProgress},FinalApplicationStatus:${stateReport.getFinalApplicationStatus.toString},StartTime:${startTime}"
        ApplicationState("Successed", msg)
      }

      //2016-06-12 15:39:34-Topic:test,ApplicationState:FINISHED,Progress:1.0 FinalApplicationStatus:SUCCEEDED,StartTime:2016-06-08 17:23:08
      buildStatReportMsg
    }

    def submitApplication(): ApplicationState = {
      println("yarnClient.submitApplication(appContext: ApplicationSubmissionContext)")
      ApplicationState("Successed", s"${applicationIdstr} submitted")
    }

    val applicationState =
      try {
        OperTypeEnums.withName(oper) match {
          case KILL   => killApplication
          case QUERY  => queryApplication
          case SUBMIT => submitApplication
        }
      } catch {
        case e: ApplicationNotFoundException => ApplicationState("Failed", s"${applicationIdstr} Not Found")
        case _                               => ApplicationState("Error", s"${applicationIdstr} Error While Operating")
      }

    //TODO 是否发送告警
    println(applicationState)

    yarnClient.stop()
    
    applicationState
  }
}
