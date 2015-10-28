#!/bin/sh
##run etl project main class ETLRunner

if [ $# -lt 1 ]
then
  echo "usage:bin/etl-spark.sh configFile=conf/etl-adsl.xml  processId=adsl YYYYMM=201505"
  exit -1
fi

SPARK_HOME=/home/chenc/tt/spark
SPARK_URL=spark://10.17.35.16:7077

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

  RUN_CMD="$SPARK_HOME/bin/spark-submit --master $SPARK_URL --class com.gsta.bigdata.etl.ETLRunner lib/etl-1.0-jar-with-dependencies.jar $@"
  echo $RUN_CMD
  eval $RUN_CMD
fi

