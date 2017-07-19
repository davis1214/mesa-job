topic=$1
appid=$2

# appid="application_1459494992574_28179"

spark_submit="/usr/local/webserver/spark-1.5.1-bin-2.6.0/bin/spark-submit"
classname="com.di.mesa.monitor.YarnAplicationMonitor"
jar_packages="/home/www/jj/kafka_2.10-0.8.2.1.jar,/home/www/jj/kafka-clients-0.8.2.1.jar,/home/www/jj/mysql-connector-java-5.1.12.jar,/home/www/jj/bonecp-0.8.0.RELEASE.jar,/usr/local/webserver/spark-1.5.1-bin-2.6.1/lib/spark-examples-1.5.1-hadoop2.6.0.jar"
jar_package="/home/www/jj/rt-job-1.0-SNAPSHOT.jar"


${spark_submit} --class $classname --master yarn-client --num-executors 6 --queue spark --total-executor-cores 3 --driver-memory 2G --jars jar_packages $jar_package $topic $appid


