#!/usr/bin/env bash

####################################
#   rt-job driver scripts  rt-public-job-driver.sh
#   author davihe
#
#   增加规范信息
#
####################################
source /etc/profile

#--env set
export SPARK_HOME="/usr/local/webserver/spark-1.5.1-bin-2.6.0/"
export JARS="/home/www/jj/kafka_2.10-0.8.2.1.jar,/home/www/jj/kafka-clients-0.8.2.1.jar,/home/www/jj/mysql-connector-java-5.1.12.jar,/home/www/jj/bonecp-0.8.0.RELEASE.jar,/usr/local/webserver/spark-1.5.1-bin-2.6.1/lib/spark-examples-1.5.1-hadoop2.6.0.jar"

CLASS_NAME="com.di.mesa.streamingjob.monitor.rts.UdcLogJob"
APP_JAR="/home/www/jj/rt-monitor.jar"
QUEUE="spark"
MASTER="yarn-client"
LOG="/home/www/jj//spark-streaming.log"

TOPIC="app_udc_account"
ZOOKEEPERS="localhost:2181"
BROKERS="localhost:9092"


log_format(){
 log=$1
 echo " info `date "+%Y-%m-%d %H:%M:%S"` |" $1
}

function print_usage(){
  echo "Usage: rt-public-job-driver.sh [--conf key=value] COMMAND"
  echo "       where COMMAND is one of:"
  echo "  topic                kafka topic which is necessary"
  echo "  zk[zookeeper]        address of zookeepers which is necessary"
  echo "  broker               address of kafka brokers which is necessary"
  echo "  queue                queue of spark"
  echo "  master               master"
  echo "  jar                  application jar"
  echo "  duration             duration"
  echo "  groupid              groupid"
  echo ""
}

if [ $# -lt 3 ];then
    print_usage
    exit -1
else
    while [ $# -ne 0 ];do
        case $1 in
           -topic|--topic)
               shift
               TOPIC=$1
               ;;
           -zk|--zookeeper)
               shift
               ZOOKEEPERS=$1
               ;;
           -broker|--broker)
               shift
               BROKERS=$1
               ;;
           -queue|--queue)
               shift
               QUEUE=$1
               ;;
           -master|--master)
               shift
               MASTER=$1
               ;;
           -jar|--jar)
               shift
               JAR=$1
               ;;
        esac
        shift
    done
fi


if [ -z $TOPIC ];then
    log_format "topic must be identified"
    exit -1
fi
if [ -z $ZOOKEEPERS ];then
    ZOOKEEPERS="10.1.24.100:2181,10.1.24.101:2181,10.1.24.102:2181"
fi
if [ -z $BROKERS ];then
    BROKERS="10.1.24.100:9092,10.1.24.101:9092,10.1.24.102:9092"
fi
if [ -z $QUEUE ];then
    QUEUE="spark"
fi
if [ -z $MASTER ];then
    MASTER="yarn-client"
fi
if [ -z $JAR ];then
    JAR="/home/www/jj/rt-monitor.jar"
fi
if [ -z $DURATION ];then
    DURATION="30"
fi
if [ -z $GROUPID ];then
    GROUPID=$TOPIC"_groupid"
fi


log_format "sh rt-public-job-driver.sh --topic $TOPIC --zookeeper $ZOOKEEPERS --broker $BROKERS --queue $QUEUE --master $MASTER --jar $JAR"

# -- nohup $SPARK_HOME/bin/spark-submit --conf spark.serializer=org.apache.spark.serializer.KryoSerializer --class ${CLASS_NAME}  --master $MASTER --queue $QUEUE --num-executors 6  --total-executor-cores 3 --driver-memory 2G --jars $JARS ${APP_JAR} -zks $ZOOKEEPERS -duration $DURATION -brokers $BROKERS -topic $TOPIC -groupid $GROUPID > $LOG 2>&1 &


#--submit_job
echo "nohup $SPARK_HOME/bin/spark-submit --conf spark.serializer=org.apache.spark.serializer.KryoSerializer --class ${CLASS_NAME}  --master $MASTER --queue $QUEUE --num-executors 6  --total-executor-cores 3 --driver-memory 2G --jars $JARS ${APP_JAR} -zks $ZOOKEEPERS -duration $DURATION -brokers $BROKERS -topic $TOPIC -groupid $GROUPID > $LOG 2>&1 &"

#-- get application id




