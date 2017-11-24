#!/bin/bash
export  LANG="en_US.UTF-8"
export  SUPPORTED="zh_CN.UTF-8:zh_CN:zh:en_US.UTF-8:en_US:en"
export  SYSFONT="latarcyrheb-sun16"

basepath=$(cd "$(dirname "$0")"; pwd)

jmxport=$1
while [ $# -gt 1 ]; do
    COMMAND=$2
    case $COMMAND in
      -debug)
      DEBUG_MODE="true"
      shift
      ;;
    *)
      break
      ;;
    esac
done


if [ "x"$jmxport = "x" ];then
  jmxport=8902
fi

hostip=`/sbin/ifconfig eth0 | awk '/inet addr/ {print $2}' | cut -f2 -d ":"`

echo "host $hostip port $jmxport"
JAVA_HOME="/usr/local/webserver/jdk1.7.0_67"
CLASSPATH="${basepath}/di-monitor-com.di.mesa.metric.alarm-1.0-SNAPSHOT.jar:."
JAVA_MAIN="com.di.monitor.com.di.mesa.metric.AlarmServer"
JVM_ARGS="-Xms1500m -Xmx1500m -Xmn1024m -XX:SurvivorRatio=15 -XX:PermSize=64m -XX:MaxPermSize=256m -XX:ParallelGCThreads=8 -XX:+UseConcMarkSweepGC -XX:CMSInitiatingOccupancyFraction=85"
JMX_ARGS="-Dcom.sun.management.jmxremote=true -Djava.rmi.server.hostname=${hostip} -Dcom.sun.management.jmxremote.port=${jmxport} -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false"

if [ "x$DEBUG_MODE" = "xtrue" ]; then
  echo "${JAVA_HOME}/bin/java -classpath ${CLASSPATH} ${JAVA_MAIN} -conf /home/www/di-monitor-com.di.mesa.metric.alarm/conf"
  ${JAVA_HOME}/bin/java -Dfile.encoding=UTF-8 -classpath ${CLASSPATH} ${JMX_ARGS} -server ${JVM_ARGS} ${JAVA_MAIN} -conf ${basepath}/conf
else
  echo "${JAVA_HOME}/bin/java -classpath ${CLASSPATH} ${JAVA_MAIN} -conf /home/www/di-monitor-com.di.mesa.metric.alarm/conf "
  nohup  ${JAVA_HOME}/bin/java -classpath ${CLASSPATH} -Dfile.encoding=UTF-8 ${JMX_ARGS}  -server ${JAVA_MAIN} -conf ${basepath}/conf > ./logs/server.log 2>&1 &
fi

sleep 2
jps -ml|grep ${JAVA_MAIN}

