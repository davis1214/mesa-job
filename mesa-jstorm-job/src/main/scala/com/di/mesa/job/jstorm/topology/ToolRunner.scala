package com.di.mesa.job.jstorm.topology

import com.di.mesa.job.jstorm.configure.{GenericOptionsParser, MesaConfig}
import org.slf4j.{Logger, LoggerFactory}
import org.apache.commons.cli.ParseException


/**
  * Created by davi on 17/8/3.
  */
class ToolRunner {

  @transient lazy private val LOG: Logger = LoggerFactory.getLogger(getClass)

  def run(tool: Tool, args: Array[String]) {
    run(tool.getConf, tool, args)
  }

  def run(conf: MesaConfig, tool: Tool, args: Array[String]) {
    try {
      val parser: GenericOptionsParser = new GenericOptionsParser(conf, args)
      LOG.info(conf.toString)
      tool.run(parser.getRemainingArgs)
    } catch {
      case e: ParseException => {
        LOG.error("Error parsing generic options:" + e.getMessage, e)
        GenericOptionsParser.printGenericCommandUsage(System.err)
        System.exit(2)
      }
      case e: Exception => {
        LOG.error("Error running tool", e)
        System.exit(1)
      }
    }
  }

}
