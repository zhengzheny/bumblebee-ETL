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
  #run hadoop map/reduce
  BASEDIR=`dirname "$0"`/..
  cd $BASEDIR

  RUN_CMD="hadoop jar lib/etl-1.0-jar-with-dependencies.jar $@"
  echo $RUN_CMD
  eval $RUN_CMD
fi

