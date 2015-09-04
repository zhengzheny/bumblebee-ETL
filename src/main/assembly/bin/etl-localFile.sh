#!/bin/sh
##run etl project main class ETLRunner

if [ $# -lt 1 ]
then
  echo "usage:bin/etl.sh configFile=conf/etl-adsl.xml  processId=adsl YYYYMM=201505"
  exit -1
fi

paras=`echo $@  | awk '{for(i=1;i<=NF;i++) print $i}' `
for para in $paras
do
  if [[ $para =~ "configFile" ]]
  then
    configFile=`echo $para | cut -d'=' -f 2`
  fi
done

if [ -n "$configFile" ]
then  
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
  RUN_CMD="$RUN_CMD -Xmx4G -Xms4G "
  #RUN_CMD="$RUN_CMD -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.authenticate=false "
  #RUN_CMD="$RUN_CMD -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.port=1984 "
  RUN_CMD="$RUN_CMD com.gsta.bigdata.etl.ETLRunner $@"
  
  echo $RUN_CMD
  eval $RUN_CMD
fi

