package common

import scopt.OptionParser
import org.apache.hadoop.mapred.JobID

object ParamParser {

  case class JobParam(zkQuorum: String = "localhost:2181", brokerlist: String = "localhost:9092", topic: String = "test1", groupId: String = "test1_groupid", duration: Long = 30, rate: String = "1000", jobId: String = "jobid0001")

  def main(args: Array[String]): Unit = {

    val parser = new OptionParser[JobParam]("Collaborative Filtering") {
      head("Basic Job Param Filter")
      opt[String]("topic")
        .required()
        .text(s"topic")
        .action((x, c) => c.copy(topic = x))
      opt[String]("zks")
        .required()
        .text(s"zookeeper Quorum")
        .action((x, c) => c.copy(zkQuorum = x))
      opt[String]("brokers")
        .required()
        .text(s"broker lists")
        .action((x, c) => c.copy(brokerlist = x))
      opt[Long]("duration")
        .required()
        .text(s"windows size of spark streaming")
        .action((x, c) => c.copy(duration = x))

      opt[String]("groupid")
        .text(s"groupid,default is topic name")
        .action((x, c) => c.copy(groupId = x))
      opt[String]("jobid")
        .text(s"job id")
        .action((x, c) => c.copy(jobId = x))
      opt[String]("rate")
        .text(s"maxRate of per-partition to fetch message from kafka, default 1000")
        .action((x, c) => c.copy(rate = x))

      note(
        """
          |Usage:
          | -topic,<arg>         topic
          | -zks,<arg>           zookeeper Quorum
          | -brokers,<arg>       broker lists
          | -duration, <arg>     windows size of spark streaming
          | -groupid, <arg>      groupid
          | -jobid ,<arg>        jobId
          | -rate,<arg>          maxRate of per-partition to fetch message from kafka, default 1000
          | 
          | 
          | /usr/local/webserver/spark-1.5.1-bin-2.6.0/bin/spark-submit 
          |   --conf spark.serializer=org.apache.spark.serializer.KryoSerializer 
          |   --class com.di.mesa.streamingjob.monitor.job.PublicRtJob
          |   --master yarn-client --num-executors 6 --queue spark --total-executor-cores 3 --driver-memory 2G 
          |   --jars /home/www/jj/kafka_2.10-0.8.2.1.jar,/home/www/jj/kafka-clients-0.8.2.1.jar,/home/www/jj/mysql-connector-java-5.1.12.jar,/home/www/jj/bonecp-0.8.0.RELEASE.jar,/usr/local/webserver/spark-1.5.1-bin-2.6.1/lib/spark-examples-1.5.1-hadoop2.6.0.jar /home/www/jj/rt-job-1.0-SNAPSHOT.jar -zks localhost:2181
          |   -duration 30 
          |   -brokers localhost:9092
          |   -topic app_udc_account 
          |   -groupid app_udc_account_groupid0001
          |   -jobid jobid0001
        """.stripMargin)
    }

    val defaultParams = JobParam()
    parser.parse(args, defaultParams).map { params =>
      println("param:" + params)
    } getOrElse {
      System.exit(1)
    }

    val jobParam = parser.parse(args, defaultParams).getOrElse{
      System.exit(1)
    }
    
    
    println("execute over")

  }

}