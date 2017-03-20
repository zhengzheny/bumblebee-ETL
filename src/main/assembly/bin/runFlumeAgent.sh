#start run flume agent,every agent deals with one data source directry
#data source directory instances from configure template
#every flume agent process has itself log file

if [ $# -lt 3 ]
then
  echo "usage:bin/runFlumeAgent.sh configFile type agentNumber"
  exit -1
fi

BASEDIR=`dirname "$0"`/..
cd $BASEDIR

#configFile=./conf/file2kafka-S1udns.conf 
configFile=$1
type=$2
flumeAgentNum=$3

workPath=/data/huaweidpi/work/$type/
logFile=./conf/log4j.properties
configPath=./conf/instance/$type
logPath=/data/logs/$type

if [ ! -d "$configPath" ]
then
  mkdir -p $configPath
else
  rm -f $configPath/*
fi

if [ ! -d "$logPath" ]
then
  mkdir -p $logPath
fi

for((i=0;i<$flumeAgentNum;i++))
do
  srcPath="$workPath/$type$i"
  config=$configPath/$type$i.conf
  sed "s:GSTA_SOURCE_SPOOL_DIR:${srcPath}:g" $configFile > $config
  
  log=$logPath/$type$i.log
  logConf=$configPath/$type$i.log4j.properties
  sed "s:GSTA_LOG_FILE_NAME:${log}:g" $logFile > $logConf

  nohup bin/etl-flume.sh $config -Dlog4j.configuration=file:$logConf &

done