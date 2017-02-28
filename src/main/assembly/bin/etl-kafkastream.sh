#!/bin/sh
##run etl project main class ETLRunner

if [ $# -lt 2 ]
then
  echo "usage:bin/etl-kafkastream.sh configFile streamAgentNo"
  exit -1
fi

configFile=$1
no=$2        

BASEDIR=`dirname "$0"`/..
cd $BASEDIR

BIGDATA_CLASSPATH="$BASEDIR/conf:$BASEDIR/lib/"
for i in "$BASEDIR"/lib/*.jar
do
  BIGDATA_CLASSPATH="$BIGDATA_CLASSPATH:$i"
done

#RUN_CMD="\"$JAVA_HOME/bin/java\""
RUN_CMD="java "
RUN_CMD="$RUN_CMD -classpath \"$BIGDATA_CLASSPATH\""
RUN_CMD="$RUN_CMD -Xmx2G -Xms2G "

logConf="./conf/instance/log4j"$no".properties"                                             
sed "s:LOG_FILE:/home/kafkastream/logs/stream${no}.log:g" ./conf/log4j.properties > $logConf
RUN_CMD="$RUN_CMD -Dlog4j.configuration=file:$logConf "

RUN_CMD="$RUN_CMD com.gsta.bigdata.etl.ETLRunner $configFile"
                               
echo $RUN_CMD
eval $RUN_CMD


