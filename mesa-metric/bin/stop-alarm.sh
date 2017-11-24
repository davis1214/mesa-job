#!/bin/bash
export  LANG="en_US.UTF-8"
export  SUPPORTED="zh_CN.UTF-8:zh_CN:zh:en_US.UTF-8:en_US:en"
export  SYSFONT="latarcyrheb-sun16"

jmxport=$1
if [ "x"$jmxport = "x" ];then
  jmxport=8902
fi

hostip=`/sbin/ifconfig eth0 | awk '/inet addr/ {print $2}' | cut -f2 -d ":"`
echo "host $hostip port $jmxport"
JAVA_HOME="/usr/local/webserver/jdk1.7.0_67"
JMX_ARGS="-Dcom.sun.management.jmxremote.port=${jmxport} -Djava.rmi.server.hostname=${hostip} -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.authenticate=false"
CLASSPATH="/home/www/di-monitor-com.di.mesa.metric.alarm/di-monitor-com.di.mesa.metric.alarm-1.0-SNAPSHOT.jar:."
JAVA_MAIN="com.di.monitor.common.mbean.JmxShutDownHandler"
APP_NAME="com.di.monitor.com.di.mesa.metric.alarm:type=AlarmServer-1"
JMX_ARGS="-Dmonitor.server.jmx.host=${hostip} -Dmonitor.server.jmx.port=${jmxport} -Djava.security.policy=jstatd.all.policy"
JAVA_TOKEN="com.di.monitor.com.di.mesa.metric.AlarmServer"

echo "${JAVA_HOME}/bin/java -Dfile.encoding=UTF-8 -classpath ${CLASSPATH} -Dmonitor.server.object.name=${APP_NAME} ${JMX_ARGS} ${JAVA_MAIN}"
${JAVA_HOME}/bin/java -Dfile.encoding=UTF-8 -classpath ${CLASSPATH} -Dmonitor.server.object.name=${APP_NAME} ${JMX_ARGS} ${JAVA_MAIN}


function check(){
  should_stop=false
  sleep_second=2
  while [ "x"${should_stop} == "xfalse"  ];
  do
     pid=`ps -ef | grep -v grep | grep di-monitor-alarm |grep ${JAVA_MAIN}| awk '{print $2}'`
     if [ "x"${pid} == "x" ];then
        should_stop=true
     else
        echo "thread ${JAVA_TOKEN} with pid ${pid} still exists ,after ${sleep_second} seconds will restart check"
     fi
     sleep ${sleep_second}
 done
}


echo ""
check
echo "stopped!!!"


